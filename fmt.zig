const std = @import("std");
const zodbc = @import("zodbc");

const DEC_BUF_LEN = 2 * (2 + std.math.log10(std.math.maxInt(u128)));
pub inline fn decToString(dec: zodbc.c.SQL_NUMERIC_STRUCT) !struct { [DEC_BUF_LEN]u8, []const u8 } {
    const middle: comptime_int = @divExact(DEC_BUF_LEN, 2);
    var buf: [DEC_BUF_LEN]u8 = undefined;
    var buf_slice: []u8 = @constCast(buf[0..]);
    const printed = try std.fmt.bufPrint(
        buf_slice[middle..],
        "{}",
        .{@as(u128, @bitCast(dec.val))},
    );

    var start: u8 = middle;
    var end: u8 = middle + @as(u8, @intCast(printed.len));

    const scale: u8 = @intCast(dec.scale);
    if (scale == 0) {} else if (scale < printed.len) {
        for (0..scale) |i| {
            buf[end - i] = buf[end - i - 1];
        }
        buf[end - scale] = '.';
        end += 1;
    } else {
        const diff = scale - @as(u8, @intCast(printed.len));
        for (0..diff) |i| {
            buf[start - 1 - i] = '0';
        }
        buf[start - 1 - diff] = '.';
        buf[start - 2 - diff] = '0';
        start -= diff + 2;
    }

    switch (dec.sign) {
        1 => {},
        0 => {
            buf[start - 1] = '-';
            start -= 1;
        },
        else => unreachable,
    }

    return .{ buf, buf[start..end] };
}

const TimePrec = enum { s, ms, us, ns };
fn TimeString(precision: TimePrec) type {
    return [
        switch (precision) {
            .s => "12:34:56".len,
            .ms => "12:34:56.123".len,
            .us => "12:34:56.123456".len,
            .ns => "12:34:56.123456789".len,
        }
    ]u8;
}
pub inline fn timeToString(
    comptime precision: TimePrec,
    hour: u8,
    minute: u8,
    second: u8,
    frac: if (precision == .s) void else u32,
) TimeString(precision) {
    var ret: TimeString(precision) = undefined;
    // comptime std.debug.assert(c_type == .char);
    // const days = @divFloor(a_val, dt_info.?.frac * 60 * 60 * 24);
    // const time = @mod(a_val, dt_info.?.frac * 60 * 60 * 24);
    // std.debug.assert(days == 0);
    const out = std.fmt.bufPrint(&ret, switch (precision) {
        .s => "{:0>2}:{:0>2}:{:0>2}",
        .ms => "{:0>2}:{:0>2}:{:0>2}.{:0>3}",
        .us => "{:0>2}:{:0>2}:{:0>2}.{:0>6}",
        .ns => "{:0>2}:{:0>2}:{:0>2}.{:0>9}",
    }, .{
        hour,
        minute,
        second,
        switch (precision) {
            .s => {},
            .ms => @divFloor(frac, 1_000_000),
            .us => @divFloor(frac, 1_000),
            .ns => frac,
        },
    }) catch unreachable;
    std.debug.assert(out.len == ret.len);
    return ret;
}

const DateString = ["0001-01-01".len]u8;
pub inline fn dateToString(year: u16, month: u8, day: u8) DateString {
    var ret: DateString = undefined;
    const out = std.fmt.bufPrint(
        &ret,
        "{:0>4}-{:0>2}-{:0>2}",
        .{ year, month, day },
    ) catch unreachable;
    std.debug.assert(out.len == ret.len);
    return ret;
}
