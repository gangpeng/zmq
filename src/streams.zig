/// ZMQ Kafka Streams
///
/// Stream processing client library:
/// - Topology: DAG of processing nodes
/// - KStream: unbounded record stream with DSL (filter/map/flatMap/groupByKey)
/// - KeyValueStore: in-memory state store
/// - WindowStore: windowed aggregation state store
/// - SessionStore: session-based windowing
/// - ProcessorContext: processor execution context
/// - Serde: key/value serializer/deserializer interface
/// - StreamsConfig: topology configuration

pub const streams = @import("streams/streams.zig");
pub const Topology = streams.Topology;
pub const KStream = streams.KStream;
pub const KeyValueStore = streams.KeyValueStore;
pub const WindowStore = streams.WindowStore;
pub const SessionStore = streams.SessionStore;
pub const ProcessorContext = streams.ProcessorContext;
pub const Serde = streams.Serde;
pub const StreamsConfig = streams.StreamsConfig;
pub const Record = streams.Record;

test {
    @import("std").testing.refAllDecls(@This());
}
