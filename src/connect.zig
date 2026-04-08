/// ZMQ Kafka Connect
///
/// Connector framework for data integration:
/// - SourceConnector: reads from external systems
/// - SinkConnector: writes to external systems
/// - StandaloneWorker: single-process connector runtime
/// - FileSourceConnector: reads lines from file → topic
/// - FileSinkConnector: topic → writes lines to file
/// - ConnectorManager: lifecycle management for all connectors

pub const connect = @import("connect/connect.zig");
pub const SourceConnector = connect.SourceConnector;
pub const SinkConnector = connect.SinkConnector;
pub const StandaloneWorker = connect.StandaloneWorker;
pub const ConnectRecord = connect.ConnectRecord;
pub const FileSourceConnector = connect.FileSourceConnector;
pub const FileSinkConnector = connect.FileSinkConnector;
pub const ConnectorManager = connect.ConnectorManager;

test {
    @import("std").testing.refAllDecls(@This());
}
