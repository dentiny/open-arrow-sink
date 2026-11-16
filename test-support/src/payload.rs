/// Reproducible incompressible fixture bytes; not a cryptographic random source.
/// Zero and one share the same nonzero xorshift seed.
pub fn seeded_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.max(1);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
