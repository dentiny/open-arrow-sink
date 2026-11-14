# Open Arrow Sink 首版设计

首版 partition 写入只支持 Lance sink，并提供独立的 index 配置与构建阶段。本文约定每个逻辑 partition 对应一个 Lance dataset；同一 partition 可以由多个 fragment writer 并行写入，再合并提交。该布局是首版设计选择。

## 职责与边界

| 组件 | 职责 |
| --- | --- |
| Partition router | 根据 partition key 切分 Arrow RecordBatch，保留完整行并路由到对应 writer |
| Partition coordinator | 管理 dataset writer、背压、并发上限和各 partition 的完成状态 |
| Lance dataset writer | 收集 fragment 事务，为一个 partition 提交数据版本 |
| Lance index plan | 校验索引配置，在数据提交后调用 Lance 创建索引 |

Index 在架构上独立成层，首版代码放在 Lance backend 内部。partition coordinator 调度索引阶段，具体索引参数与 Lance API 由 index 模块封装。公共 Arrow 写入路径无需依赖 Lance index 类型；其他 sink 配置了 partition 或 index 时，在写入前返回不支持错误。

逻辑 partition、Lance fragment 和向量索引的 IVF partition 是三个不同概念。前者决定 dataset 边界，fragment 是 dataset 内的数据存储单元，IVF partition 是向量检索参数。

## 配置与接口草案

以下是接口结构草案，实际 Rust 异步接口形式在实现时确定。

```rust
struct LanceWriteOptions {
    // 写入模式、存储配置和文件大小等参数。
    indexes: Vec<LanceIndexSpec>,
}

struct LanceIndexSpec {
    column: String,
    kind: LanceIndexKind,
}

enum LanceIndexKind {
    Scalar,       // BTree
    Vector,       // IVF_FLAT，默认 L2
    Text,         // Inverted，使用 Lance 默认参数
    BloomFilter,
}

// prepare(final_storage_schema, specs) -> LanceIndexPlan
// dataset_writer.commit_data() -> CommittedDataset
// index_plan.build(committed_dataset) -> IndexBuildReport
// index_plan.retry(dataset_uri, expected_version, failed_specs)
//     -> IndexBuildReport
```

所有新建 partition 继承同一组索引定义；首版每列最多一个索引。保留参数扩展空间，但暂不提供按 partition 覆盖配置、多列索引、自定义分词和其他向量索引算法。

索引定义描述期望状态，构建结果单独记录，不能以配置存在推断索引已完成。索引名称应由列名和类型确定性生成，并检查已有同名索引是否属于该定义，避免覆盖用户创建的其他索引。

## 写入与索引生命周期

1. 计算最终落盘 schema，包括列转换和生成列，校验全部索引定义。引用不存在的列、重复列、类型不支持等错误必须在产生数据文件之前返回。
2. router 将 batch 按 key 切分。partition key 编码必须区分 null、空字符串和包含路径字符的值，禁止原始值直接拼接为存储路径。
3. 各 fragment writer 写未提交的数据，完成后把事务交给所属 dataset writer。所有 writer 必须成功结束，才能提交该 partition；任一 writer 失败都不能把该 partition 当作完整数据提交。
4. dataset writer 合并兼容事务，调用 Lance 提交数据，记录 dataset URI 和实际提交版本。
5. 在刚提交的 dataset 上逐项创建索引。partition 之间的索引任务可以受限并发；同一 dataset 的索引创建顺序执行，使用每次创建后更新的 dataset handle。
6. coordinator 汇总每个 partition 的数据与索引结果。仅在所有请求索引都成功时，返回该 partition 完整成功。

索引以各 partition 的整个 dataset 为范围，不能逐 batch 或逐 fragment 创建。首版没有跨 dataset 的全局索引，也没有跨 partition 的原子提交；查询端需要先选择 partition，再使用对应 dataset 的索引。

## 索引类型与校验

类型支持矩阵参考现有 `LanceIndexPlan`，最终以项目锁定的 Lance 版本为准。

| 类型 | 创建方式 | 写入前校验 |
| --- | --- | --- |
| Scalar | BTree | 校验参考实现允许的标量类型，不能简单放行全部 Arrow 类型 |
| Vector | IVF_FLAT，L2 | 固定长度且维度大于零的数值列表，元素类型符合锁定版本要求 |
| Text | Inverted | 字符串类型 |
| BloomFilter | Bloom filter | 单独维护支持矩阵，不能复用 Scalar 的全部类型 |

向量索引默认沿用参考实现的启发式：`num_partitions = clamp(ceil(dataset_rows / 8192), 1, 4096)`，使用该 partition 提交后的总行数。该公式不是小样本训练成功的保证；空 dataset、全 null 向量和训练样本不足必须有明确结果，首版返回具体索引构建失败及已提交版本，不静默跳过请求的索引。

## 写入模式

Create、append 和 overwrite 的作用域均为本次触达的 partition。overwrite 不删除未触达的其他 partition；整表替换需要额外的表级协议。

Append 后，请求的索引必须覆盖提交后的完整 dataset，首版可以通过重建完成。已有索引若未出现在本次请求中，不承诺已覆盖新增 fragments；结果只报告本次请求的索引状态。后续可增加显式增量维护能力。

首次提交前记录 append 的基准版本，按 Lance 的冲突机制处理并发提交，不自行吞掉冲突后重复写数据。索引阶段遇到其他写入者改变 dataset 时，返回冲突及可恢复信息；首版不承诺与外部 writer 并发修改同一 dataset 的完整协调。

没有输入行时不创建逻辑 partition。显式创建空 dataset 的能力应由非 partition 写入接口另行定义。

## 失败结果与恢复

每个 partition 的结果包含 `partition_key`、dataset URI、行数、`data_version`、最新已知版本，以及每个请求索引的名称、状态和错误。数据提交结果必须保留，即使后续索引失败也随整体错误返回。

| 失败阶段 | 可见状态 | 恢复方式 |
| --- | --- | --- |
| 配置校验 | 尚未写入 | 修正配置后重新开始 |
| Fragment 写入 | 当前 partition 未提交，可能有孤立文件 | 确认未提交后重试写入，孤立文件按 Lance 清理策略处理 |
| 数据提交响应不确定 | 可能已经提交 | 先确认提交结果；无法确认时报告状态未知，禁止自动重放 append |
| 索引构建 | 数据已经提交，部分索引可能成功 | 校验 dataset 版本及实际索引状态，只重试失败索引 |

运行内维护索引结果；进程重启后，调用者需要保留结果，或通过 dataset 的索引元数据重新确认状态。首版不声称提供持久化作业调度或 exactly once 写入。

Index retry 必须核对预期版本和已有索引定义。若 dataset 已变化，返回版本冲突，由调用方显式发起针对新版本的维护；不能把旧任务继续执行的结果算作原版本成功。索引创建也可能提交新版本，因此需同时记录最初的数据版本和索引完成后的版本。

## 并发与资源限制

fragment 写入与索引构建分别设置并发上限。coordinator 必须限制活跃 writer 数和缓冲字节，超限时对上游施加背压。partition key 基数很高时可以轮换 fragment writer，但仍由同一个 dataset writer 汇总事务，不能因为淘汰 writer 就提前提交 partition。

首版索引在该 partition 的所有数据提交后构建，避免每次 batch 或 writer 轮换都重建。完整的 partition 元数据与未提交事务仍可能随基数增长，超出资源预算时应明确报错，不能宣称仅限制活跃 writer 就实现了总内存有界。

## 验证计划

- 多 partition 与多 fragment writer：每个 dataset 的行数、归属及索引覆盖范围正确。
- 无效列名、重复定义、错误类型：在写入任何 partition 之前失败。
- 使用 Lance 元数据确认索引类型和覆盖信息；执行实际标量、文本及向量查询验证结果，不能仅统计版本文件数量。
- 注入索引失败：保留已提交数据和成功索引，索引重试不重复 append。
- 覆盖空输入、小样本向量、append 重建、局部 overwrite、提交响应不确定和并发版本冲突。
- 验证 writer 与索引任务并发上限，以及高基数 partition 的背压或预算错误。

## 参考实现

参考版本为 `dentiny/duckdb_lance_conversion` 的提交 `ba481d653f64875d5825d1b5409ed281206a74d1`。

- [index.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance/index.rs)：索引类型、写入前校验、Lance 创建参数与向量 partition 数计算。
- [dataset.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance/dataset.rs)：fragment 事务合并，先提交数据再创建索引。
- [lance.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance.rs)：写入配置与 dataset writer、fragment writer 的分工。

本设计在参考实现之上补充 partition 调度、分阶段结果和索引重试语义。
