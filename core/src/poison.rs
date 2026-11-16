use crate::{Error, Result};

/// Terminal-state primitive for backend implementations.
/// Decorators should use `GuardedWriter` or `GuardedDataWriter` to share the
/// complete lifecycle implementation rather than managing this flag themselves.
///
/// Call `begin` before validation or I/O and `complete` only after a successful
/// write. Errors and cancellation then leave the writer poisoned automatically.
/// A final operation such as `DataWriter::finish` never calls `complete`.
/// This tracks usability only: resource ownership and cleanup remain with the writer.
#[derive(Debug, Default)]
pub struct Poison {
    poisoned: bool,
}

impl Poison {
    pub fn check(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    /// Check usability and poison before any fallible or asynchronous work.
    pub fn begin(&mut self) -> Result<()> {
        self.check()?;
        self.poison();
        Ok(())
    }

    /// Restore usability after the operation begun by `begin` succeeds.
    pub fn complete(&mut self) {
        self.poisoned = false;
    }

    /// Make the writer terminal, for example when aborting or closing it.
    pub fn poison(&mut self) {
        self.poisoned = true;
    }
}
