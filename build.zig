const std = @import("std");

pub fn build(b: *std.Build) void {
    // ---------------------------------------------------------------
    // Target & optimisation
    // ---------------------------------------------------------------
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const time_compat_mod = b.addModule("time_compat", .{
        .root_source_file = b.path("src/core/time_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const posix_compat_mod = b.addModule("posix_compat", .{
        .root_source_file = b.path("src/core/posix_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const mutex_compat_mod = b.addModule("mutex_compat", .{
        .root_source_file = b.path("src/core/mutex_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const list_compat_mod = b.addModule("list_compat", .{
        .root_source_file = b.path("src/core/list_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const fs_compat_mod = b.addModule("fs_compat", .{
        .root_source_file = b.path("src/core/fs_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const net_compat_mod = b.addModule("net_compat", .{
        .root_source_file = b.path("src/core/net_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    const random_compat_mod = b.addModule("random_compat", .{
        .root_source_file = b.path("src/core/random_compat.zig"),
        .target = target,
        .optimize = optimize,
    });

    mutex_compat_mod.addImport("time_compat", time_compat_mod);

    // ---------------------------------------------------------------
    // Modules – each major subsystem is its own module
    // ---------------------------------------------------------------

    // Core utilities (byte buffers, varint, crc32c, uuid, config, logging)
    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("src/core.zig"),
        .target = target,
        .optimize = optimize,
    });
    core_mod.addImport("time_compat", time_compat_mod);
    core_mod.addImport("posix_compat", posix_compat_mod);
    core_mod.addImport("mutex_compat", mutex_compat_mod);
    core_mod.addImport("list_compat", list_compat_mod);
    core_mod.addImport("fs_compat", fs_compat_mod);
    core_mod.addImport("net_compat", net_compat_mod);
    core_mod.addImport("random_compat", random_compat_mod);

    // Memory allocators (arena, pool, slab, tracking)
    const allocators_mod = b.addModule("allocators", .{
        .root_source_file = b.path("src/allocators.zig"),
        .target = target,
        .optimize = optimize,
    });
    allocators_mod.addImport("time_compat", time_compat_mod);
    allocators_mod.addImport("posix_compat", posix_compat_mod);
    allocators_mod.addImport("mutex_compat", mutex_compat_mod);
    allocators_mod.addImport("list_compat", list_compat_mod);
    allocators_mod.addImport("fs_compat", fs_compat_mod);
    allocators_mod.addImport("net_compat", net_compat_mod);
    allocators_mod.addImport("random_compat", random_compat_mod);

    // Concurrency primitives (Future, ConcurrentMap, Channel, Scheduler)
    const concurrency_mod = b.addModule("concurrency", .{
        .root_source_file = b.path("src/concurrency.zig"),
        .target = target,
        .optimize = optimize,
    });
    concurrency_mod.addImport("time_compat", time_compat_mod);
    concurrency_mod.addImport("posix_compat", posix_compat_mod);
    concurrency_mod.addImport("mutex_compat", mutex_compat_mod);
    concurrency_mod.addImport("list_compat", list_compat_mod);
    concurrency_mod.addImport("fs_compat", fs_compat_mod);
    concurrency_mod.addImport("net_compat", net_compat_mod);
    concurrency_mod.addImport("random_compat", random_compat_mod);
    concurrency_mod.addImport("core", core_mod);

    // Compression codecs (gzip, lz4, zstd, snappy wrappers)
    const compression_mod = b.addModule("compression", .{
        .root_source_file = b.path("src/compression.zig"),
        .target = target,
        .optimize = optimize,
    });
    compression_mod.addImport("time_compat", time_compat_mod);
    compression_mod.addImport("posix_compat", posix_compat_mod);
    compression_mod.addImport("mutex_compat", mutex_compat_mod);
    compression_mod.addImport("list_compat", list_compat_mod);
    compression_mod.addImport("fs_compat", fs_compat_mod);
    compression_mod.addImport("net_compat", net_compat_mod);
    compression_mod.addImport("random_compat", random_compat_mod);
    compression_mod.addImport("core", core_mod);

    // Wire protocol (code-generated message types, headers, record batches)
    const protocol_mod = b.addModule("protocol", .{
        .root_source_file = b.path("src/protocol.zig"),
        .target = target,
        .optimize = optimize,
    });
    protocol_mod.addImport("time_compat", time_compat_mod);
    protocol_mod.addImport("posix_compat", posix_compat_mod);
    protocol_mod.addImport("mutex_compat", mutex_compat_mod);
    protocol_mod.addImport("list_compat", list_compat_mod);
    protocol_mod.addImport("fs_compat", fs_compat_mod);
    protocol_mod.addImport("net_compat", net_compat_mod);
    protocol_mod.addImport("random_compat", random_compat_mod);
    protocol_mod.addImport("core", core_mod);
    protocol_mod.addImport("compression", compression_mod);

    // Security (TLS, SASL/PLAIN, SCRAM-SHA-256, ACL, JWT)
    const security_mod = b.addModule("security", .{
        .root_source_file = b.path("src/security.zig"),
        .target = target,
        .optimize = optimize,
    });
    security_mod.addImport("time_compat", time_compat_mod);
    security_mod.addImport("posix_compat", posix_compat_mod);
    security_mod.addImport("mutex_compat", mutex_compat_mod);
    security_mod.addImport("list_compat", list_compat_mod);
    security_mod.addImport("fs_compat", fs_compat_mod);
    security_mod.addImport("net_compat", net_compat_mod);
    security_mod.addImport("random_compat", random_compat_mod);
    security_mod.addImport("core", core_mod);

    // Network layer (io_uring/epoll server, TLS, connection management)
    const network_mod = b.addModule("network", .{
        .root_source_file = b.path("src/network.zig"),
        .target = target,
        .optimize = optimize,
    });
    network_mod.addImport("time_compat", time_compat_mod);
    network_mod.addImport("posix_compat", posix_compat_mod);
    network_mod.addImport("mutex_compat", mutex_compat_mod);
    network_mod.addImport("list_compat", list_compat_mod);
    network_mod.addImport("fs_compat", fs_compat_mod);
    network_mod.addImport("net_compat", net_compat_mod);
    network_mod.addImport("random_compat", random_compat_mod);
    network_mod.addImport("core", core_mod);
    network_mod.addImport("protocol", protocol_mod);
    network_mod.addImport("concurrency", concurrency_mod);
    network_mod.addImport("security", security_mod);

    // Storage engine (WAL, LogCache, S3BlockCache, S3 object format)
    const storage_mod = b.addModule("storage", .{
        .root_source_file = b.path("src/storage.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_mod.addImport("time_compat", time_compat_mod);
    storage_mod.addImport("posix_compat", posix_compat_mod);
    storage_mod.addImport("mutex_compat", mutex_compat_mod);
    storage_mod.addImport("list_compat", list_compat_mod);
    storage_mod.addImport("fs_compat", fs_compat_mod);
    storage_mod.addImport("net_compat", net_compat_mod);
    storage_mod.addImport("random_compat", random_compat_mod);
    storage_mod.addImport("core", core_mod);
    storage_mod.addImport("concurrency", concurrency_mod);
    storage_mod.addImport("compression", compression_mod);
    storage_mod.addImport("security", security_mod);

    // Raft consensus (KRaft state machine, election, replication)
    const raft_mod = b.addModule("raft", .{
        .root_source_file = b.path("src/raft.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_mod.addImport("time_compat", time_compat_mod);
    raft_mod.addImport("posix_compat", posix_compat_mod);
    raft_mod.addImport("mutex_compat", mutex_compat_mod);
    raft_mod.addImport("list_compat", list_compat_mod);
    raft_mod.addImport("fs_compat", fs_compat_mod);
    raft_mod.addImport("net_compat", net_compat_mod);
    raft_mod.addImport("random_compat", random_compat_mod);
    raft_mod.addImport("core", core_mod);
    raft_mod.addImport("protocol", protocol_mod);
    raft_mod.addImport("network", network_mod);
    raft_mod.addImport("concurrency", concurrency_mod);

    // Broker (request handlers, purgatory, partition management)
    const broker_mod = b.addModule("broker", .{
        .root_source_file = b.path("src/broker.zig"),
        .target = target,
        .optimize = optimize,
    });
    broker_mod.addImport("time_compat", time_compat_mod);
    broker_mod.addImport("posix_compat", posix_compat_mod);
    broker_mod.addImport("mutex_compat", mutex_compat_mod);
    broker_mod.addImport("list_compat", list_compat_mod);
    broker_mod.addImport("fs_compat", fs_compat_mod);
    broker_mod.addImport("net_compat", net_compat_mod);
    broker_mod.addImport("random_compat", random_compat_mod);
    broker_mod.addImport("core", core_mod);
    broker_mod.addImport("protocol", protocol_mod);
    broker_mod.addImport("network", network_mod);
    broker_mod.addImport("storage", storage_mod);
    broker_mod.addImport("raft", raft_mod);
    broker_mod.addImport("security", security_mod);
    broker_mod.addImport("concurrency", concurrency_mod);
    broker_mod.addImport("compression", compression_mod);

    // ---------------------------------------------------------------
    // Main executable – the AutoMQ broker
    // ---------------------------------------------------------------
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "core", .module = core_mod },
            .{ .name = "allocators", .module = allocators_mod },
            .{ .name = "concurrency", .module = concurrency_mod },
            .{ .name = "compression", .module = compression_mod },
            .{ .name = "protocol", .module = protocol_mod },
            .{ .name = "network", .module = network_mod },
            .{ .name = "storage", .module = storage_mod },
            .{ .name = "raft", .module = raft_mod },
            .{ .name = "broker", .module = broker_mod },
            .{ .name = "security", .module = security_mod },
            .{ .name = "time_compat", .module = time_compat_mod },
            .{ .name = "posix_compat", .module = posix_compat_mod },
            .{ .name = "mutex_compat", .module = mutex_compat_mod },
            .{ .name = "list_compat", .module = list_compat_mod },
            .{ .name = "fs_compat", .module = fs_compat_mod },
            .{ .name = "net_compat", .module = net_compat_mod },
            .{ .name = "random_compat", .module = random_compat_mod },
        },
    });
    const exe = b.addExecutable(.{
        .name = "zmq",
        .root_module = exe_mod,
    });

    // C compression libraries are optional — Zig's std.compress provides
    // gzip/zlib and zstd. LZ4 and Snappy will use C FFI when available.
    // For now, build without C library dependencies.
    // TODO (Phase 0.5): Add optional C library linking for lz4 and snappy
    // exe.linkSystemLibrary("z");
    // exe.linkSystemLibrary("lz4");
    // exe.linkSystemLibrary("zstd");
    // exe.linkSystemLibrary("snappy");

    b.installArtifact(exe);

    // ---------------------------------------------------------------
    // Run step
    // ---------------------------------------------------------------
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the ZMQ broker");
    run_step.dependOn(&run_cmd.step);

    // ---------------------------------------------------------------
    // Unit tests – one per module
    // ---------------------------------------------------------------
    const test_step = b.step("test", "Run all unit tests");

    const test_modules = .{
        .{ "src/core.zig", "core" },
        .{ "src/allocators.zig", "allocators" },
        .{ "src/concurrency.zig", "concurrency" },
        .{ "src/compression.zig", "compression" },
        .{ "src/protocol.zig", "protocol" },
        .{ "src/network.zig", "network" },
        .{ "src/storage.zig", "storage" },
        .{ "src/raft.zig", "raft" },
        .{ "src/broker.zig", "broker" },
        .{ "src/security.zig", "security" },
        .{ "src/streams.zig", "streams" },
        .{ "src/connect.zig", "connect" },
        .{ "src/tools.zig", "tools" },
        .{ "src/config.zig", "config" },
    };

    inline for (test_modules) |entry| {
        const test_mod = b.createModule(.{
            .root_source_file = b.path(entry[0]),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "time_compat", .module = time_compat_mod },
                .{ .name = "posix_compat", .module = posix_compat_mod },
                .{ .name = "mutex_compat", .module = mutex_compat_mod },
                .{ .name = "list_compat", .module = list_compat_mod },
                .{ .name = "fs_compat", .module = fs_compat_mod },
                .{ .name = "net_compat", .module = net_compat_mod },
                .{ .name = "random_compat", .module = random_compat_mod },
                .{ .name = "core", .module = core_mod },
                .{ .name = "security", .module = security_mod },
                .{ .name = "broker", .module = broker_mod },
                .{ .name = "protocol", .module = protocol_mod },
                .{ .name = "network", .module = network_mod },
                .{ .name = "storage", .module = storage_mod },
                .{ .name = "raft", .module = raft_mod },
            },
        });
        const t = b.addTest(.{
            .root_module = test_mod,
        });

        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Integration test: production readiness validation
    {
        const integration_mod = b.createModule(.{
            .root_source_file = b.path("tests/production_readiness_test.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "core", .module = core_mod },
                .{ .name = "storage", .module = storage_mod },
                .{ .name = "security", .module = security_mod },
                .{ .name = "network", .module = network_mod },
                .{ .name = "time_compat", .module = time_compat_mod },
                .{ .name = "posix_compat", .module = posix_compat_mod },
                .{ .name = "mutex_compat", .module = mutex_compat_mod },
                .{ .name = "list_compat", .module = list_compat_mod },
                .{ .name = "fs_compat", .module = fs_compat_mod },
                .{ .name = "net_compat", .module = net_compat_mod },
                .{ .name = "random_compat", .module = random_compat_mod },
            },
        });
        const integration_test = b.addTest(.{
            .root_module = integration_mod,
        });

        const run_integration = b.addRunArtifact(integration_test);
        test_step.dependOn(&run_integration.step);
    }

    // MinIO/S3 integration tests are intentionally not part of the default
    // unit suite. Run with:
    //   ZMQ_RUN_MINIO_TESTS=1 zig build test-minio
    const minio_test_step = b.step("test-minio", "Run MinIO/S3-backed integration tests");
    {
        const minio_mod = b.createModule(.{
            .root_source_file = b.path("tests/minio_s3_test.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "storage", .module = storage_mod },
                .{ .name = "broker", .module = broker_mod },
                .{ .name = "time_compat", .module = time_compat_mod },
                .{ .name = "posix_compat", .module = posix_compat_mod },
                .{ .name = "mutex_compat", .module = mutex_compat_mod },
                .{ .name = "list_compat", .module = list_compat_mod },
                .{ .name = "fs_compat", .module = fs_compat_mod },
                .{ .name = "net_compat", .module = net_compat_mod },
                .{ .name = "random_compat", .module = random_compat_mod },
                .{ .name = "core", .module = core_mod },
                .{ .name = "security", .module = security_mod },
                .{ .name = "protocol", .module = protocol_mod },
                .{ .name = "network", .module = network_mod },
                .{ .name = "raft", .module = raft_mod },
            },
        });
        const minio_test = b.addTest(.{
            .root_module = minio_mod,
        });

        const run_minio = b.addRunArtifact(minio_test);
        minio_test_step.dependOn(&run_minio.step);
    }

    // External broker-process crash/restart harness. The Python test skips
    // unless ZMQ_RUN_PROCESS_CRASH_TESTS=1 is set, so the build step is safe to
    // execute in local/CI environments without MinIO.
    const s3_process_crash_step = b.step("test-s3-process-crash", "Run gated S3 broker process crash/restart harness");
    {
        const run_s3_process_crash = b.addSystemCommand(&.{ "python3", "tests/s3_process_crash_test.py" });
        run_s3_process_crash.step.dependOn(b.getInstallStep());
        s3_process_crash_step.dependOn(&run_s3_process_crash.step);
    }

    // External Kafka-client compatibility matrix. The Python test skips unless
    // ZMQ_RUN_CLIENT_MATRIX=1 is set and requires a running broker plus client
    // binaries such as kcat or Kafka CLI tools.
    const client_matrix_step = b.step("test-client-matrix", "Run gated Kafka client compatibility matrix");
    {
        const run_client_matrix = b.addSystemCommand(&.{ "python3", "tests/client_matrix_test.py" });
        run_client_matrix.step.dependOn(b.getInstallStep());
        client_matrix_step.dependOn(&run_client_matrix.step);
    }

    // ---------------------------------------------------------------
    // Benchmark step
    // ---------------------------------------------------------------
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("benchmarks/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
        .imports = &.{
            .{ .name = "core", .module = core_mod },
            .{ .name = "allocators", .module = allocators_mod },
            .{ .name = "concurrency", .module = concurrency_mod },
            .{ .name = "time_compat", .module = time_compat_mod },
            .{ .name = "posix_compat", .module = posix_compat_mod },
            .{ .name = "mutex_compat", .module = mutex_compat_mod },
            .{ .name = "list_compat", .module = list_compat_mod },
            .{ .name = "fs_compat", .module = fs_compat_mod },
            .{ .name = "net_compat", .module = net_compat_mod },
            .{ .name = "random_compat", .module = random_compat_mod },
        },
    });
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });

    const bench_run = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run performance benchmarks");
    bench_step.dependOn(&bench_run.step);
}
