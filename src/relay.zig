// Relay.zig - Transaction relay and broadcast module
// Named after the relay stations that rapidly transmitted messages across vast distances

const std = @import("std");
const yam = @import("root.zig");
const Courier = @import("courier.zig").Courier;

/// Timing strategy for transaction broadcast
pub const TimingStrategy = enum {
    /// Send to all peers simultaneously (fastest propagation)
    simultaneous,
    /// Stagger sends with random delays (better privacy)
    staggered_random,
};

/// Options for broadcast operation
pub const BroadcastOptions = struct {
    strategy: TimingStrategy = .simultaneous,
    min_delay_ms: u32 = 100,
    max_delay_ms: u32 = 5000,
};

/// Result of a broadcast attempt to a single peer
pub const BroadcastReport = struct {
    peer: yam.PeerInfo,
    success: bool,
    rejected: bool,
    reject_reason: ?[]u8,
    elapsed_ms: u64,

    pub fn deinit(self: *BroadcastReport, allocator: std.mem.Allocator) void {
        if (self.reject_reason) |reason| {
            allocator.free(reason);
        }
    }
};

/// Result of broadcasting to multiple peers
pub const BroadcastResult = struct {
    reports: []BroadcastReport,
    success_count: usize,
    reject_count: usize,

    pub fn deinit(self: *BroadcastResult, allocator: std.mem.Allocator) void {
        for (self.reports) |*report| {
            report.deinit(allocator);
        }
        allocator.free(self.reports);
    }
};

/// Context passed to each connection worker thread
const ConnectContext = struct {
    courier: *Courier,
    line_num: usize,
    total_lines: usize,
};

/// Worker function for parallel peer connection
fn connectWorker(ctx: ConnectContext) void {
    const addr_str = ctx.courier.peer.format();
    const addr = std.mem.sliceTo(&addr_str, ' ');

    // Calculate how many lines to move up to reach our line
    // Lines are printed top-to-bottom, cursor ends at bottom
    const lines_up = ctx.total_lines - ctx.line_num;

    ctx.courier.connect() catch |err| {
        // Move up, update with FAILED, move back down
        std.debug.print("\x1b[{d}A\r{s}: FAILED ({s})\x1b[K\x1b[{d}B\r", .{
            lines_up,
            addr,
            @errorName(err),
            lines_up,
        });
        return;
    };

    // Move up, update with OK, move back down
    std.debug.print("\x1b[{d}A\r{s}: OK\x1b[K\x1b[{d}B\r", .{
        lines_up,
        addr,
        lines_up,
    });
}

/// Relay manages connections to multiple peers for transaction broadcast
pub const Relay = struct {
    couriers: []Courier,
    allocator: std.mem.Allocator,

    pub fn init(peers: []const yam.PeerInfo, allocator: std.mem.Allocator) !Relay {
        const couriers = try allocator.alloc(Courier, peers.len);
        errdefer allocator.free(couriers);

        for (couriers, peers) |*courier, peer| {
            courier.* = Courier.init(peer, allocator);
        }

        return .{
            .couriers = couriers,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Relay) void {
        for (self.couriers) |*courier| {
            courier.deinit();
        }
        self.allocator.free(self.couriers);
    }

    /// Connect to all peers in parallel, performing handshake
    /// Returns the number of successful connections
    pub fn connectAll(self: *Relay) usize {
        const n = self.couriers.len;
        if (n == 0) return 0;

        // Print initial status for all peers
        for (self.couriers) |*courier| {
            const addr_str = courier.peer.format();
            std.debug.print("{s}: Connecting...\n", .{std.mem.sliceTo(&addr_str, ' ')});
        }

        // Allocate thread handles
        const threads = self.allocator.alloc(?std.Thread, n) catch {
            // Fallback to sequential if allocation fails
            return self.connectAllSequential();
        };
        defer self.allocator.free(threads);

        // Spawn a thread for each courier
        for (self.couriers, 0..) |*courier, i| {
            const ctx = ConnectContext{
                .courier = courier,
                .line_num = i,
                .total_lines = n,
            };
            threads[i] = std.Thread.spawn(.{}, connectWorker, .{ctx}) catch null;
        }

        // Wait for all threads to complete
        for (threads) |maybe_thread| {
            if (maybe_thread) |thread| thread.join();
        }

        // Count successful connections
        var connected: usize = 0;
        for (self.couriers) |courier| {
            if (courier.connected) connected += 1;
        }
        return connected;
    }

    /// Sequential fallback for connectAll
    fn connectAllSequential(self: *Relay) usize {
        var connected: usize = 0;
        for (self.couriers) |*courier| {
            courier.connect() catch continue;
            connected += 1;
        }
        return connected;
    }

    /// Broadcast a transaction to all connected peers
    pub fn broadcastTx(self: *Relay, tx_bytes: []const u8, options: BroadcastOptions) !BroadcastResult {
        var reports = try self.allocator.alloc(BroadcastReport, self.couriers.len);
        errdefer self.allocator.free(reports);

        var success_count: usize = 0;
        var reject_count: usize = 0;

        // Initialize RNG for staggered timing
        var rng_seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&rng_seed));
        var rng = std.Random.DefaultPrng.init(rng_seed);

        for (self.couriers, 0..) |*courier, i| {
            // Apply delay for staggered strategy
            if (options.strategy == .staggered_random and i > 0) {
                const delay = rng.random().intRangeAtMost(u32, options.min_delay_ms, options.max_delay_ms);
                std.Thread.sleep(@as(u64, delay) * std.time.ns_per_ms);
            }

            const start = std.time.milliTimestamp();
            var report = BroadcastReport{
                .peer = courier.peer,
                .success = false,
                .rejected = false,
                .reject_reason = null,
                .elapsed_ms = 0,
            };

            if (!courier.connected) {
                report.elapsed_ms = @intCast(std.time.milliTimestamp() - start);
                reports[i] = report;
                continue;
            }

            // Send transaction
            courier.sendTx(tx_bytes) catch |err| {
                std.debug.print("Failed to send tx to peer: {s}\n", .{@errorName(err)});
                report.elapsed_ms = @intCast(std.time.milliTimestamp() - start);
                reports[i] = report;
                continue;
            };

            // Wait briefly for potential reject message
            const reject_result = courier.waitForReject(1000) catch null;

            if (reject_result) |reject| {
                report.rejected = true;
                report.reject_reason = reject;
                reject_count += 1;
            } else {
                report.success = true;
                success_count += 1;
            }

            report.elapsed_ms = @intCast(std.time.milliTimestamp() - start);
            reports[i] = report;
        }

        return .{
            .reports = reports,
            .success_count = success_count,
            .reject_count = reject_count,
        };
    }
};

/// Print a formatted broadcast report
pub fn printBroadcastReport(reports: []const BroadcastReport, allocator: std.mem.Allocator) void {
    _ = allocator;

    std.debug.print("\n=== Broadcast Report ===\n", .{});

    for (reports) |report| {
        const addr_str = report.peer.format();
        const status = if (report.success)
            "SUCCESS"
        else if (report.rejected)
            "REJECTED"
        else
            "FAILED";

        std.debug.print("{s}: {s} ({d}ms)\n", .{
            std.mem.sliceTo(&addr_str, ' '),
            status,
            report.elapsed_ms,
        });

        if (report.reject_reason) |reason| {
            std.debug.print("  Reason: {s}\n", .{reason});
        }
    }
}
