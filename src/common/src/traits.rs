use std::hash;

/// Trait for messages that have a routing key
pub trait KeyedMessage {
    type RoutingKey<'a>: hash::Hash
    where
        Self: 'a;
    /// Returns a reference to the bytes of a keyed message
    fn routing_key(&self) -> Self::RoutingKey<'_>;
}
