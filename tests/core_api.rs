use duovium::{Aggregate, AppendError, Event, Executor, InMemoryEventStore, Metadata, Proposed};
use std::convert::Infallible;

// Deliberately has no serialization implementation: persistence is an adapter concern.
#[derive(Debug, Clone)]
struct Added(u32);
impl Event for Added {
    fn name(&self) -> &'static str {
        "counter.added"
    }
    fn version(&self) -> u32 {
        1
    }
}
#[derive(Debug)]
struct Counter;
impl Aggregate for Counter {
    type Id = String;
    type Command = u32;
    type Event = Added;
    type Error = Infallible;
    type State = u32;
    fn initial_state(&self) -> u32 {
        0
    }
    fn apply(&self, state: u32, event: &Added) -> u32 {
        state + event.0
    }
    fn handle(&self, _: &u32, command: u32) -> Result<Vec<Proposed<Added>>, Infallible> {
        Ok(vec![Proposed {
            event: Added(command),
            metadata: Metadata::new(),
        }])
    }
}

#[tokio::test]
async fn a_non_serializable_event_works_in_the_functional_core()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = Executor::new(Counter, InMemoryEventStore::new(), 1)?;
    let id = "counter".to_owned();
    executor.execute(&id, 7, Metadata::new()).await?;
    assert_eq!(executor.load(&id).await?.0, 7);
    Ok(())
}

#[test]
fn zero_attempts_are_rejected_at_construction() {
    assert!(Executor::new(Counter, InMemoryEventStore::<String, Added>::new(), 0).is_err());
}

#[test]
fn append_errors_preserve_their_source() {
    use std::error::Error;
    let error = AppendError::Store(std::io::Error::other("disk unavailable"));
    assert_eq!(error.source().unwrap().to_string(), "disk unavailable");
}
