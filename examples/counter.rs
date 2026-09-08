use duovium::{Aggregate, Event, Executor, InMemoryEventStore, Metadata, Proposed};
use std::convert::Infallible;

#[derive(Debug, Clone)]
struct Added(i64);

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
    type Command = i64;
    type Event = Added;
    type Error = Infallible;
    type State = i64;

    fn initial_state(&self) -> i64 {
        0
    }
    fn apply(&self, state: i64, event: &Added) -> i64 {
        state + event.0
    }
    fn handle(&self, _: &i64, amount: i64) -> Result<Vec<Proposed<Added>>, Infallible> {
        Ok(vec![Proposed {
            event: Added(amount),
            metadata: Metadata::new(),
        }])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Executor::new(Counter, InMemoryEventStore::new(), 3)?;
    let id = "visits".to_owned();
    executor.execute(&id, 4, Metadata::new()).await?;
    let (state, version) = executor.load(&id).await?;
    assert_eq!(state, 4);
    assert_eq!(version.get(), 1);
    println!("Counter: {state}, stream version: {}", version.get());
    Ok(())
}
