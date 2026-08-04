use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

#[derive(Clone, Debug)]
pub struct CapturedEvent {
    pub metadata: &'static Metadata<'static>,
    pub fields: BTreeMap<&'static str, String>,
}

#[derive(Clone, Default)]
pub struct EventCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl EventCapture {
    pub fn layer(&self) -> CaptureLayer {
        CaptureLayer {
            capture: self.clone(),
        }
    }

    pub fn with_default<T>(&self, f: impl FnOnce() -> T) -> T {
        tracing::subscriber::with_default(Registry::default().with(self.layer()), f)
    }

    pub fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().expect("event capture poisoned").clone()
    }

    pub fn take_events(&self) -> Vec<CapturedEvent> {
        std::mem::take(&mut *self.events.lock().expect("event capture poisoned"))
    }
}

pub struct CaptureLayer {
    capture: EventCapture,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        self.capture
            .events
            .lock()
            .expect("event capture poisoned")
            .push(CapturedEvent {
                metadata: event.metadata(),
                fields: visitor.fields,
            });
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<&'static str, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields.insert(field.name(), format!("{value:?}"));
    }
}

#[test]
fn limits_events_per_callsite() {
    let capture = EventCapture::default();

    capture.with_default(|| {
        for attempt in 0..3 {
            restate_tracing::info_ratelimited!(
                2,
                Duration::MAX,
                attempt = attempt,
                "rate-limited event"
            );
        }
    });

    let events = capture.events();

    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.metadata.level() == &Level::INFO)
    );
    assert_eq!(events[0].fields["attempt"], "0");
    assert_eq!(events[1].fields["attempt"], "1");
}

#[test]
fn supression_label() {
    let capture = EventCapture::default();

    capture.with_default(|| {
        for _ in 0..2 {
            for attempt in 0..20 {
                restate_tracing::info_ratelimited!(
                    2,
                    Duration::from_millis(100),
                    attempt = attempt,
                    "rate-limited event"
                );
            }
            std::thread::sleep(Duration::from_millis(110));
        }
    });

    let events = capture.events();

    assert_eq!(events.len(), 4);
    assert!(
        events
            .iter()
            .all(|event| event.metadata.level() == &Level::INFO)
    );
    assert_eq!(events[0].fields["attempt"], "0");
    assert_eq!(events[0].fields.get("restate.logging.suppressed"), None);

    assert_eq!(events[1].fields["attempt"], "1");
    assert_eq!(events[1].fields.get("restate.logging.suppressed"), None);

    // Only first event after period expiry, gets a label with the number of suppressed events
    assert_eq!(events[2].fields["attempt"], "0");
    assert_eq!(events[2].fields["restate.logging.suppressed"], "18");

    assert_eq!(events[3].fields["attempt"], "1");
    assert_eq!(events[3].fields.get("restate.logging.suppressed"), None);
}
