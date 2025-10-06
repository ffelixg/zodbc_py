const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;
const PyFuncs = @import("PyFuncs.zig");
const fmt = @import("fmt.zig");
const utils = @import("utils.zig");
const pyCall = utils.pyCall;
const Dt7Fetch = utils.Dt7Fetch;

const CDataType = zodbc.odbc.types.CDataType;

const Conversions = union(enum) {
    // char: CDataType.char.Type(),
    begin_row: void,
    end_row: void,

    wchar: CDataType.wchar.Type(),
    sshort: CDataType.sshort.Type(),
    ushort: CDataType.ushort.Type(),
    slong: CDataType.slong.Type(),
    ulong: CDataType.ulong.Type(),
    float: CDataType.float.Type(),
    double: CDataType.double.Type(),
    bit: CDataType.bit.Type(),
    stinyint: CDataType.stinyint.Type(),
    utinyint: CDataType.utinyint.Type(),
    sbigint: CDataType.sbigint.Type(),
    ubigint: CDataType.ubigint.Type(),
    binary: CDataType.binary.Type(),
    numeric: CDataType.numeric.Type(),
    guid: CDataType.guid.Type(),
    type_date: CDataType.type_date.Type(),
    type_time: CDataType.type_time.Type(),
    type_timestamp_micro: CDataType.type_timestamp.Type(),
    type_timestamp_string: CDataType.type_timestamp.Type(),
    ss_time2_micro: CDataType.ss_time2.Type(),
    ss_time2_string: CDataType.ss_time2.Type(),
    ss_timestampoffset_micro: CDataType.ss_timestampoffset.Type(),
    ss_timestampoffset_string: CDataType.ss_timestampoffset.Type(),

    const Tags = @typeInfo(@This()).@"union".tag_type.?;

    fn Type(tag: Tags) type {
        return @FieldType(@This(), @tagName(tag));
    }

    fn asTypeValue(comptime tag: Tags, data: []u8) Type(tag) {
        return std.mem.bytesToValue(Type(tag), data);
    }
};

cycle: []Conversions.Tags,

pub fn init(res: *zodbc.ResultSet, allocator: std.mem.Allocator, dt7_fetch: Dt7Fetch) !@This() {
    const cycle = try allocator.alloc(Conversions.Tags, res.n_cols + 1);
    errdefer allocator.free(cycle);
    for (res.columns.items, 0..) |col, it| {
        cycle[it] = switch (col.c_type) {
            inline .type_timestamp, .ss_time2, .ss_timestampoffset => |c_type| blk: {
                switch (dt7_fetch) {
                    .micro, .nano => {
                        const tag = comptime std.enums.nameCast(Conversions.Tags, @tagName(c_type) ++ "_micro");
                        comptime std.debug.assert(c_type.Type() == Conversions.Type(tag));
                        break :blk tag;
                    },
                    .string => {
                        const prec = try res.stmt.colAttribute(@intCast(it + 1), .precision);
                        if (prec <= 6) {
                            const tag = comptime std.enums.nameCast(Conversions.Tags, @tagName(c_type) ++ "_micro");
                            comptime std.debug.assert(c_type.Type() == Conversions.Type(tag));
                            break :blk tag;
                        } else {
                            const tag = comptime std.enums.nameCast(Conversions.Tags, @tagName(c_type) ++ "_string");
                            comptime std.debug.assert(c_type.Type() == Conversions.Type(tag));
                            break :blk tag;
                        }
                    },
                }
            },
            inline else => |c_type| blk: {
                @setEvalBranchQuota(0xFFFF_FFFF);
                const tag: ?Conversions.Tags = comptime for (std.enums.values(Conversions.Tags)) |tag| {
                    if (std.mem.eql(u8, @tagName(tag), @tagName(c_type))) {
                        std.debug.assert(c_type.Type() == Conversions.Type(tag));
                        break tag;
                    }
                } else null;
                break :blk tag orelse return error.ConversionNotImplemented;
            },
        };
    }
    cycle[cycle.len - 1] = .end_row;
    return .{ .cycle = cycle };
}

pub fn deinit(self: *const @This(), allocator: std.mem.Allocator) void {
    allocator.free(self.cycle);
}

pub fn fetch_py(
    self: *const @This(),
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
    var rows = try std.ArrayListUnmanaged(Obj).initCapacity(
        allocator,
        n_rows orelse 64,
    );
    defer rows.deinit(allocator);
    defer for (rows.items) |row| c.Py_DECREF(row);
    var i_col: usize = 0;
    var i_row: usize = 0;
    sw: switch (Conversions.Tags.begin_row) {
        .begin_row => {
            {
                const thread_state = c.PyEval_SaveThread();
                defer c.PyEval_RestoreThread(thread_state);

                if (try res.borrowRow() == null)
                    break :sw;
            }
            try rows.append(
                allocator,
                switch (row_type) {
                    .tuple => c.PyTuple_New(@intCast(res.n_cols)) orelse return py.PyErr,
                    .dict => c.PyDict_New() orelse return py.PyErr,
                    .named => c.PyStructSequence_New(named_tuple_type) orelse return py.PyErr,
                },
            );
            std.debug.assert(i_col == 0);
            continue :sw self.cycle[0];
        },
        .end_row => {
            i_row += 1;
            if (n_rows) |n| {
                if (i_row >= n) {
                    break :sw;
                }
            }
            i_col = 0;
            continue :sw .begin_row;
        },
        inline else => |conv| {
            const py_val = if (res.borrowed_row[i_col]) |bytes|
                try odbcToPy(bytes, conv, py_funcs)
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
            continue :sw self.cycle[i_col];
        },
    }

    const py_ret = c.PyList_New(@intCast(rows.items.len)) orelse return py.PyErr;
    errdefer c.Py_DECREF(py_ret);
    for (rows.items, 0..) |row, ix| {
        if (c.PyList_SetItem(py_ret, @intCast(ix), c.Py_NewRef(row)) == -1)
            return py.PyErr;
    }
    return py_ret;
}

inline fn odbcToPy(
    bytes: []u8,
    comptime conv: Conversions.Tags,
    py_funcs: *const PyFuncs,
) !Obj {
    const val = Conversions.asTypeValue(conv, bytes);

    switch (conv) {
        .bit, .binary, .wchar => {},
        else => switch (@typeInfo(Conversions.Type(conv))) {
            .int, .float => return try @call(.always_inline, py.zig_to_py, .{val}),
            else => {},
        },
    }

    switch (conv) {
        .wchar => {
            const str = try std.unicode.wtf16LeToWtf8Alloc(
                std.heap.c_allocator,
                @as([]u16, @ptrCast(@alignCast(bytes))),
            );
            defer std.heap.c_allocator.free(str);
            return c.PyUnicode_FromStringAndSize(str.ptr, @intCast(str.len)) orelse return py.PyErr;
        },
        .binary => {
            return c.PyBytes_FromStringAndSize(bytes.ptr, @intCast(bytes.len)) orelse return py.PyErr;
        },
        .type_date => {
            return try pyCall(py_funcs.cls_date, .{ val.year, val.month, val.day });
        },
        .type_time => {
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second });
        },
        .ss_time2_micro => {
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000) });
        },
        .ss_time2_string => {
            const time_str = fmt.timeToString(9, @intCast(val.hour), @intCast(val.minute), @intCast(val.second), @intCast(val.fraction));
            return c.PyUnicode_FromStringAndSize(
                time_str ++ "",
                time_str.len,
            ) orelse py.PyErr;
        },
        .ss_timestampoffset_micro => {
            const td = try pyCall(py_funcs.cls_timedelta, .{
                0,
                @as(i32, val.timezone_hour) * 3600 + @as(i32, val.timezone_minute) * 60,
            });
            const tz = try pyCall(py_funcs.cls_timezone, .{td});
            return try pyCall(py_funcs.cls_time, .{ val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000), tz });
        },
        .ss_timestampoffset_string => {
            const dt_str = fmt.dateToString(@intCast(val.year), @intCast(val.month), @intCast(val.day));
            const time_str = fmt.timeToString(9, @intCast(val.hour), @intCast(val.minute), @intCast(val.second), @intCast(val.fraction));
            const tz_str = fmt.timezoneToString(@intCast(val.timezone_hour), @intCast(val.timezone_minute));
            return c.PyUnicode_FromStringAndSize(
                dt_str ++ " " ++ time_str ++ " " ++ tz_str,
                dt_str.len + 1 + time_str.len + 1 + tz_str.len,
            ) orelse py.PyErr;
        },
        .type_timestamp_micro => {
            return try pyCall(py_funcs.cls_datetime, .{ val.year, val.month, val.day, val.hour, val.minute, val.second, @divTrunc(val.fraction, 1000) });
        },
        .type_timestamp_string => {
            const dt_str = fmt.dateToString(@intCast(val.year), @intCast(val.month), @intCast(val.day));
            const time_str = fmt.timeToString(9, @intCast(val.hour), @intCast(val.minute), @intCast(val.second), @intCast(val.fraction));
            return c.PyUnicode_FromStringAndSize(
                dt_str ++ "T" ++ time_str,
                dt_str.len + 1 + time_str.len,
            ) orelse py.PyErr;
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
            const dec_buf, const dec_start, const dec_end = try fmt.decToString(val);
            return try pyCall(py_funcs.cls_decimal, .{dec_buf[dec_start..dec_end]});
        },
        .bit => {
            return try py.zig_to_py(switch (val) {
                1 => true,
                0 => false,
                else => unreachable,
            });
        },
        else => return try py.zig_to_py(val),
        // else => @compileError("missing conversion for Conversion.Tag: " ++ @tagName(conv)),
    }
    comptime unreachable;
}
