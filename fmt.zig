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

pub fn TimeString(precision: comptime_int) type {
    std.debug.assert(precision >= 0 and precision <= 9);
    return ["12:34:56.".len + precision]u8;
}
pub inline fn timeToString(
    comptime precision: comptime_int,
    hour: u8,
    minute: u8,
    second: u8,
    frac: if (precision == 0) void else u32,
) TimeString(precision) {
    var ret: TimeString(precision) = undefined;
    const precision_str = std.fmt.comptimePrint("{}", .{precision});
    const out = std.fmt.bufPrint(
        &ret,
        if (precision == 0)
            "{:0>2}:{:0>2}:{:0>2}"
        else
            "{:0>2}:{:0>2}:{:0>2}.{:0>" ++ precision_str ++ "}",
        .{
            hour, minute, second, if (precision == 0)
                void{}
            else
                frac,
        },
    ) catch unreachable;
    std.debug.assert(out.len == ret.len);
    return ret;
}

pub const DateString = ["0001-01-01".len]u8;
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

pub inline fn timezoneToString(timezone_hour: i8, timezone_minute: u8) [6]u8 {
    var ret: [6]u8 = undefined;
    const out = std.fmt.bufPrint(
        &ret,
        "{c}{:0>2}:{:0>2}",
        .{
            if (timezone_hour >= 0) @as(u8, '+') else @as(u8, '-'),
            @as(u8, @intCast(if (timezone_hour >= 0) timezone_hour else -timezone_hour)),
            timezone_minute,
        },
    ) catch unreachable;
    std.debug.assert(out.len == ret.len);
    return ret;
}

pub inline fn parseDecimal(dec: []const u8) !zodbc.c.SQL_NUMERIC_STRUCT {
    var ix: u8 = 0;
    const sign: u1 = switch (dec[0]) {
        '-' => blk: {
            ix += 1;
            break :blk 0;
        },
        '+' => blk: {
            ix += 1;
            break :blk 1;
        },
        else => 1,
    };
    var value: u128 = 0;
    var scale: u7 = 0;
    var encountered_dot: bool = false;
    sw: switch (dec[ix]) {
        '0'...'9' => |val| {
            scale += 1;
            if (value >= comptime std.math.maxInt(u128) / 10 - 255 + '0')
                return error.InvalidDecimal;
            value = 10 * value + val - '0';
            ix += 1;
            if (ix == dec.len) {
                @branchHint(.unlikely);
                if (!encountered_dot) {
                    scale = 0;
                }
                break :sw;
            }
            continue :sw dec[ix];
        },
        '.' => {
            scale = 0;
            ix += 1;
            if (ix == dec.len or encountered_dot)
                return error.InvalidDecimal;
            encountered_dot = true;
            continue :sw dec[ix];
        },
        'e', 'E' => {
            if (!encountered_dot) {
                scale = 0;
            }
            ix += 1;
            if (ix >= dec.len - 1)
                return error.InvalidDecimal4;
            const exp_value = std.fmt.parseInt(u7, dec[ix + 1 ..], 10) catch return error.InvalidDecimal3;
            switch (dec[ix]) {
                '-' => {
                    scale += exp_value;
                },
                '+' => {
                    if (scale < exp_value) {
                        value = std.math.mul(
                            u128,
                            value,
                            std.math.pow(u128, 10, exp_value - scale),
                        ) catch return error.InvalidDecimal2;
                        scale = 0;
                    } else {
                        scale -= exp_value;
                    }
                },
                else => return error.InvalidDecimal1,
            }
        },
        else => return error.InvalidDecimal,
    }
    const precision: u7 = @intCast(@max(
        scale,
        std.math.log10(value) + 1,
    ));
    std.debug.print("dec: {s}, value: {}, scale: {}, precision: {}\n", .{
        dec, value, scale, precision,
    });
    return .{
        .val = @bitCast(value),
        .sign = sign,
        .precision = precision,
        .scale = scale,
    };
}
