use open_arrow_sink_core::{Error, Poison};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

#[test]
fn cancelling_an_in_flight_operation_leaves_it_terminal() {
    let mut state = Poison::default();
    let mut write = Box::pin(async {
        state.begin()?;
        std::future::pending::<()>().await;
        state.complete();
        Ok::<_, Error>(())
    });
    assert!(matches!(
        write.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(write);
    assert!(matches!(state.begin(), Err(Error::Poisoned)));
}
