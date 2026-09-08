use std::fmt::Debug;

use super::{Event, Proposed};

/// A functional decision model: fold events into state, then decide new events.
///
/// `initial_state`, `apply`, and `handle` must be deterministic and free of
/// externally observable side effects. The executor may call `handle` again
/// after an optimistic conflict, using the same command and fresh history.
/// Put request time, randomness, and other decision inputs in the command;
/// perform external work separately after persistence, with delivery guarantees
/// appropriate to the application.
pub trait Aggregate: Debug + Send + Sync {
    type Id;
    type Command;
    type Event: Event;
    type Error;
    type State;

    fn initial_state(&self) -> Self::State;
    fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State;
    fn handle(
        &self,
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Vec<Proposed<Self::Event>>, Self::Error>;

    fn rehydrate(&self, events: impl Iterator<Item = Self::Event>) -> Self::State {
        events.fold(self.initial_state(), |s, e| self.apply(s, &e))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde::{Deserialize, Serialize};

    use super::Aggregate;
    use crate::{Event, Proposed};

    #[derive(Debug)]
    struct ArithmeticAggregate;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum ArithmeticEvent {
        Add(i64),
        Multiply(i64),
    }

    impl Event for ArithmeticEvent {
        fn name(&self) -> &'static str {
            match self {
                Self::Add(_) => "arithmetic.added",
                Self::Multiply(_) => "arithmetic.multiplied",
            }
        }

        fn version(&self) -> u32 {
            1
        }
    }

    impl Aggregate for ArithmeticAggregate {
        type Id = ();
        type Command = ();
        type Event = ArithmeticEvent;
        type Error = Infallible;
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State {
            match event {
                ArithmeticEvent::Add(value) => state + value,
                ArithmeticEvent::Multiply(value) => state * value,
            }
        }

        fn handle(
            &self,
            _state: &Self::State,
            _command: Self::Command,
        ) -> Result<Vec<Proposed<Self::Event>>, Self::Error> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn empty_history_rehydrates_to_default_state() {
        let state = ArithmeticAggregate.rehydrate(Vec::new().into_iter());

        assert_eq!(state, 0);
    }

    #[test]
    fn history_is_rehydrated_in_event_order() {
        let events = vec![
            ArithmeticEvent::Add(2),
            ArithmeticEvent::Multiply(3),
            ArithmeticEvent::Add(1),
        ];

        let state = ArithmeticAggregate.rehydrate(events.into_iter());

        assert_eq!(state, 7);
    }
}
