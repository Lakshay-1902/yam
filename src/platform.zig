// platform.zig - Cross-platform abstractions for Windows/POSIX differences

const std = @import("std");
const builtin = @import("builtin");

pub const is_windows = builtin.os.tag == .windows;

// =============================================================================
// Terminal Raw Mode
// =============================================================================

pub const TerminalState = if (is_windows) ?std.os.windows.DWORD else ?std.posix.termios;

pub fn setRawMode() TerminalState {
    if (is_windows) {
        const handle = std.os.windows.GetStdHandle(std.os.windows.STD_INPUT_HANDLE) catch return null;
        var mode: std.os.windows.DWORD = undefined;
        if (std.os.windows.kernel32.GetConsoleMode(handle, &mode) == 0) {
            // Not a console (e.g., piped input) - that's OK
            return null;
        }
        const original = mode;
        // Disable ENABLE_LINE_INPUT (0x0002) and ENABLE_ECHO_INPUT (0x0004)
        mode &= ~@as(std.os.windows.DWORD, 0x0006);
        if (std.os.windows.kernel32.SetConsoleMode(handle, mode) == 0) {
            return null;
        }
        return original;
    } else {
        const fd = std.fs.File.stdin().handle;
        const original = std.posix.tcgetattr(fd) catch return null;
        var raw = original;
        raw.lflag.ICANON = false;
        raw.lflag.ECHO = false;
        std.posix.tcsetattr(fd, .FLUSH, raw) catch return null;
        return original;
    }
}

pub fn restoreTerminalMode(original: TerminalState) void {
    const state = original orelse return;
    if (is_windows) {
        const handle = std.os.windows.GetStdHandle(std.os.windows.STD_INPUT_HANDLE) catch return;
        _ = std.os.windows.kernel32.SetConsoleMode(handle, state);
    } else {
        std.posix.tcsetattr(std.fs.File.stdin().handle, .FLUSH, state) catch {};
    }
}

// =============================================================================
// Socket I/O
// Windows needs send/recv from Winsock, POSIX uses write/read
// =============================================================================

pub fn socketWrite(socket: std.posix.socket_t, data: []const u8) !usize {
    if (is_windows) {
        const rc = std.os.windows.ws2_32.send(socket, data.ptr, @intCast(data.len), 0);
        if (rc == std.os.windows.ws2_32.SOCKET_ERROR) {
            return error.SocketWriteFailed;
        }
        return @intCast(rc);
    } else {
        return std.posix.write(socket, data);
    }
}

pub fn socketRead(socket: std.posix.socket_t, buf: []u8) !usize {
    if (is_windows) {
        const rc = std.os.windows.ws2_32.recv(socket, buf.ptr, @intCast(buf.len), 0);
        if (rc == std.os.windows.ws2_32.SOCKET_ERROR) {
            return error.SocketReadFailed;
        }
        if (rc == 0) {
            return 0; // Connection closed
        }
        return @intCast(rc);
    } else {
        return std.posix.read(socket, buf);
    }
}

// =============================================================================
// File Descriptor Limits
// =============================================================================

pub fn raiseFileDescriptorLimit() u64 {
    if (is_windows) {
        // Windows doesn't have rlimit - return a reasonable default
        return 1000;
    }
    const resource = std.posix.rlimit_resource.NOFILE;
    const current = std.posix.getrlimit(resource) catch return 256;

    // Try to raise soft limit to hard limit (cap at reasonable max)
    const target: u64 = if (current.max > 100000) 100000 else current.max;
    const new_limit = std.posix.rlimit{
        .cur = target,
        .max = current.max,
    };
    std.posix.setrlimit(resource, new_limit) catch {};

    // Return what we actually have now
    const final = std.posix.getrlimit(resource) catch return current.cur;
    const limit = if (final.cur > 100000) 100000 else final.cur;
    // Reserve some FDs for stdin/stdout/stderr/etc
    return if (limit > 50) limit - 50 else limit;
}
