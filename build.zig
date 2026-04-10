const std = @import("std");

pub fn build(b: *std.Build) void {
    // ---------------------------------------------------------------
    // Target & optimisation
    // ---------------------------------------------------------------
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ---------------------------------------------------------------
    // Modules – each major subsystem is its own module
    // ---------------------------------------------------------------

    // Core utilities (byte buffers, varint, crc32c, uuid, config, logging)
    const core_mod = b.addModule("core", .{
        .root_source_file = b.path("src/core.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Memory allocators (arena, pool, slab, tracking)
    const allocators_mod = b.addModule("allocators", .{
        .root_source_file = b.path("src/allocators.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Concurrency primitives (Future, ConcurrentMap, Channel, Scheduler)
    const concurrency_mod = b.addModule("concurrency", .{
        .root_source_file = b.path("src/concurrency.zig"),
        .target = target,
        .optimize = optimize,
    });
    concurrency_mod.addImport("core", core_mod);

    // Compression codecs (gzip, lz4, zstd, snappy wrappers)
    const compression_mod = b.addModule("compression", .{
        .root_source_file = b.path("src/compression.zig"),
        .target = target,
        .optimize = optimize,
    });
    compression_mod.addImport("core", core_mod);

    // Wire protocol (code-generated message types, headers, record batches)
    const protocol_mod = b.addModule("protocol", .{
        .root_source_file = b.path("src/protocol.zig"),
        .target = target,
        .optimize = optimize,
    });
    protocol_mod.addImport("core", core_mod);
    protocol_mod.addImport("compression", compression_mod);

    // Network layer (io_uring/epoll server, TLS, connection management)
    const network_mod = b.addModule("network", .{
        .root_source_file = b.path("src/network.zig"),
        .target = target,
        .optimize = optimize,
    });
    network_mod.addImport("core", core_mod);
    network_mod.addImport("protocol", protocol_mod);
    network_mod.addImport("concurrency", concurrency_mod);

    // Storage engine (WAL, LogCache, S3BlockCache, S3 object format)
    const storage_mod = b.addModule("storage", .{
        .root_source_file = b.path("src/storage.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_mod.addImport("core", core_mod);
    storage_mod.addImport("concurrency", concurrency_mod);
    storage_mod.addImport("compression", compression_mod);

    // Raft consensus (KRaft state machine, election, replication)
    const raft_mod = b.addModule("raft", .{
        .root_source_file = b.path("src/raft.zig"),
        .target = target,
        .optimize = optimize,
    });
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
    broker_mod.addImport("core", core_mod);
    broker_mod.addImport("protocol", protocol_mod);
    broker_mod.addImport("network", network_mod);
    broker_mod.addImport("storage", storage_mod);
    broker_mod.addImport("raft", raft_mod);
    broker_mod.addImport("concurrency", concurrency_mod);
    broker_mod.addImport("compression", compression_mod);

    // Security (TLS, SASL/PLAIN, SCRAM-SHA-256, ACL, JWT)
    const security_mod = b.addModule("security", .{
        .root_source_file = b.path("src/security.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ---------------------------------------------------------------
    // Main executable – the AutoMQ broker
    // ---------------------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "zmq",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("core", core_mod);
    exe.root_module.addImport("allocators", allocators_mod);
    exe.root_module.addImport("concurrency", concurrency_mod);
    exe.root_module.addImport("compression", compression_mod);
    exe.root_module.addImport("protocol", protocol_mod);
    exe.root_module.addImport("network", network_mod);
    exe.root_module.addImport("storage", storage_mod);
    exe.root_module.addImport("raft", raft_mod);
    exe.root_module.addImport("broker", broker_mod);

    // C compression libraries are optional — Zig's std.compress provides
    // gzip/zlib and zstd. LZ4 and Snappy will use C FFI when available.
    // For now, build without C library dependencies.
    // TODO (Phase 0.5): Add optional C library linking for lz4 and snappy
    // exe.linkSystemLibrary("z");
    // exe.linkSystemLibrary("lz4");
    // exe.linkSystemLibrary("zstd");
    // exe.linkSystemLibrary("snappy");

    // Link libc for TLS support (dlopen/dlsym to load OpenSSL at runtime).
    // This does NOT link OpenSSL at compile time — OpenSSL is loaded dynamically
    // at runtime only if TLS is configured. libc is needed for the dl* functions.
    exe.linkLibC();

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
        const t = b.addTest(.{
            .root_source_file = b.path(entry[0]),
            .target = target,
            .optimize = optimize,
        });
        // Link libc for modules that use dlopen (security module's OpenSSL bindings)
        t.linkLibC();

        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Integration test: production readiness validation
    {
        const integration_test = b.addTest(.{
            .root_source_file = b.path("tests/production_readiness_test.zig"),
            .target = target,
            .optimize = optimize,
        });
        integration_test.root_module.addImport("core", core_mod);
        integration_test.root_module.addImport("storage", storage_mod);
        integration_test.root_module.addImport("security", security_mod);
        integration_test.root_module.addImport("network", network_mod);
        integration_test.linkLibC();

        const run_integration = b.addRunArtifact(integration_test);
        test_step.dependOn(&run_integration.step);
    }

    // ---------------------------------------------------------------
    // Benchmark step
    // ---------------------------------------------------------------
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_source_file = b.path("benchmarks/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_exe.root_module.addImport("core", core_mod);
    bench_exe.root_module.addImport("allocators", allocators_mod);
    bench_exe.root_module.addImport("concurrency", concurrency_mod);
    bench_exe.linkLibC();

    const bench_run = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run performance benchmarks");
    bench_step.dependOn(&bench_run.step);
}
