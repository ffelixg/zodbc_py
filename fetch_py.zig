const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;
const PyFuncs = @import("PyFuncs.zig");

const CDataType = zodbc.odbc.types.CDataType;
pub fn fetch_py(
    res: *zodbc.ResultSet,
    allocator: std.mem.Allocator,
    n_rows: ?usize,
    py_funcs: *const PyFuncs,
    comptime row_type: enum { tuple, dict, named },
    names: switch (row_type) {
        .dict => [][:0]const u8,
        .tuple => void,
        .named => void,
    },
    named_tuple_type: switch (row_type) {
        .dict => void,
        .tuple => void,
        .named => *c.PyTypeObject,
    },
) !Obj {
    const cycle = try allocator.alloc(CDataType, res.n_cols + 1);
    errdefer allocator.free(cycle);
    for (res.columns.items, 0..) |col, it| {
        cycle[it] = col.c_type;
    }
    cycle[cycle.len - 1] = .default; // dummy end
    var rows = try std.ArrayListUnmanaged(Obj).initCapacity(
        allocator,
        n_rows orelse 64,
    );
    errdefer rows.deinit(allocator);
    errdefer for (rows.items) |row| c.Py_XDECREF(row);
    var i_col: usize = 0;
    var i_row: usize = 0;
    sw: switch (CDataType.ard_type) {
        // dummy start
        .ard_type => {
            if (try res.borrowRow() == null) {
                break :sw;
            }
            rows.appendAssumeCapacity(
                switch (row_type) {
                    .tuple => c.PyTuple_New(@intCast(res.n_cols)) orelse return py.PyErr,
                    .dict => c.PyDict_New() orelse return py.PyErr,
                    .named => c.PyStructSequence_New(named_tuple_type) orelse return py.PyErr,
                },
            );
            continue :sw cycle[i_col];
        },
        // dummy end
        .default => {
            i_row += 1;
            if (n_rows) |n| {
                if (i_row >= n) {
                    break :sw;
                }
            }
            i_col = 0;
            continue :sw .ard_type;
        },
        inline else => |c_type| {
            const py_val = if (res.borrowed_row[i_col]) |bytes|
                try odbcToPy(bytes, c_type, py_funcs)
            else
                c.Py_NewRef(c.Py_None());
            switch (row_type) {
                .tuple => {
                    if (c.PyTuple_SetItem(
                        rows.items[i_row],
                        @intCast(i_col),
                        py_val,
                    ) != 0) return py.PyErr;
                },
                .dict => {
                    if (c.PyDict_SetItemString(
                        rows.items[i_row],
                        names[i_col].ptr,
                        py_val,
                    ) != 0) return py.PyErr;
                },
                .named => {
                    c.PyStructSequence_SetItem(
                        rows.items[i_row],
                        @intCast(i_col),
                        py_val,
                    );
                },
            }
            i_col += 1;
            continue :sw cycle[i_col];
        },
    }

    const py_ret = c.PyList_New(@intCast(rows.items.len)) orelse return py.PyErr;
    errdefer c.Py_DECREF(py_ret);
    for (rows.items, 0..) |row, ix| {
        if (c.PyList_SetItem(py_ret, @intCast(ix), row) == -1)
            return py.PyErr;
    }
    return py_ret;
}

inline fn odbcToPy(
    bytes: []u8,
    comptime c_type: CDataType,
    py_funcs: *const PyFuncs,
) !Obj {
    const T = if (c_type.MaybeType()) |t| t else @panic("c_type not implemented: " ++ @tagName(c_type));
    const val = c_type.asTypeValue(bytes);

    switch (c_type) {
        .bit, .binary, .wchar => {},
        else => switch (@typeInfo(T)) {
            .int, .float => return try @call(.always_inline, py.zig_to_py, .{val}),
            else => {},
        },
    }

    switch (c_type) {
        .wchar => {
            const str = try std.unicode.wtf16LeToWtf8Alloc(
                std.heap.smp_allocator,
                // @as([]u16, @alignCast(@ptrCast(bytes))),
                c_type.asType(bytes),
            );
            defer std.heap.smp_allocator.free(str);
            return c.PyUnicode_FromStringAndSize(str.ptr, @intCast(str.len)) orelse return py.PyErr;
        },
        .binary => {
            return c.PyBytes_FromStringAndSize(bytes.ptr, @intCast(bytes.len)) orelse return py.PyErr;
        },
        .type_date => {
            return try pyCall(py_funcs.cls_date, .{ val.year, val.month, val.day });
        },
        .ss_time2 => {
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000) });
        },
        .type_time => {
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second });
        },
        .ss_timestampoffset => {
            const td = try pyCall(py_funcs.cls_timedelta, .{
                0,
                @as(i32, val.timezone_hour) * 3600 + @as(i32, val.timezone_minute) * 60,
            });
            const tz = try pyCall(py_funcs.cls_timezone, .{td});
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000), tz });
        },
        .type_timestamp => {
            return try pyCall(py_funcs.cls_datetime, .{ val.year, val.month, val.day, val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000) });
        },
        .guid => {
            const asbytes: [16]u8 = @bitCast(val);
            const pybytes: Obj = c.PyBytes_FromStringAndSize(
                &asbytes,
                asbytes.len,
            ) orelse return py.PyErr;
            return try pyCall(py_funcs.cls_uuid, .{ null, null, pybytes });
        },
        .numeric => {
            _, const dec_str = try decToString(val);
            return try pyCall(py_funcs.cls_decimal, .{dec_str});
        },
        .bit => {
            return try py.zig_to_py(switch (val) {
                1 => true,
                0 => false,
                else => unreachable,
            });
        },
        // else => return try py.zig_to_py(val),
        else => @compileError("missing conversion for CDataType: " ++ @tagName(c_type)),
    }
    comptime unreachable;
}

inline fn pyCall(func: Obj, args: anytype) !Obj {
    // without limited api, PyObject_Vectorcall would give better performance
    const py_args = try @call(
        .always_inline,
        py.zig_to_py,
        .{args},
    );
    defer c.Py_DECREF(py_args);
    return c.PyObject_Call(
        func,
        py_args,
        null,
    ) orelse return py.PyErr;
}

const DEC_BUF_LEN = 2 * (2 + std.math.log10(std.math.maxInt(u128)));
fn decToString(dec: zodbc.c.SQL_NUMERIC_STRUCT) !struct { [DEC_BUF_LEN]u8, []const u8 } {
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
