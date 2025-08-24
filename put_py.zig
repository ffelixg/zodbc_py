const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;
const PyFuncs = @import("PyFuncs.zig");
const CDataType = zodbc.odbc.types.CDataType;
const utils = @import("utils.zig");
const fmt = @import("fmt.zig");

pub const Conv = enum {
    wchar,
    binary,
    slong,
    sbigint,
    double,
    bit,
    numeric_string,
    guid,
    type_date,
    type_time_string,
    type_timestamp,
    // ss_timestampoffset, // TODO
    arrow_table,

    pub fn fromValue(val: Obj, funcs: PyFuncs) !Conv {
        if (c.Py_IsNone(val) == 1) {
            return .wchar;
        } else if (1 == c.PyFloat_Check(val)) {
            return .double;
        } else if (1 == c.PyBool_Check(val)) {
            return .bit;
        } else if (1 == c.PyLong_Check(val)) {
            if (try py.py_to_zig(i64, val, null) > std.math.maxInt(i32)) {
                return .sbigint;
            } else {
                return .slong;
            }
        } else if (1 == c.PyBytes_Check(val)) {
            return .binary;
        } else if (1 == c.PyUnicode_Check(val)) {
            return .wchar;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_datetime)) {
            return .type_timestamp;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_date)) {
            return .type_date;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_time)) {
            return .type_time_string;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_decimal)) {
            return .numeric_string;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_uuid)) {
            return .guid;
        } else if (1 == c.PyObject_IsInstance(val, funcs.cls_atvp)) {
            return .arrow_table;
        } else {
            return error.CouldNotFindConversion;
        }
    }
};

fn createOdbcVal(
    allocator: std.mem.Allocator,
    comptime c_type: zodbc.odbc.types.CDataType,
    val: c_type.Type(),
) ![]u8 {
    const buf = try c_type.alloc(allocator, 1);
    c_type.asType(buf)[0] = val;
    return buf;
}

const ParamList = std.ArrayListUnmanaged(struct {
    c_type: zodbc.odbc.types.CDataType,
    sql_type: zodbc.odbc.types.SQLDataType,
    ind: i64 = zodbc.c.SQL_NULL_DATA,
    data: ?[]u8,
    misc: union(enum) {
        dt: struct {
            strlen: u7,
            prec: u7,
            isstr: bool,
        },
        dec: struct {
            precision: u8,
            scale: i8,
        },
        arrow_tvp: struct {},
        varsize: void,
        noinfo: void,
    } = .noinfo,

    pub fn deinit(self: @This(), ally: std.mem.Allocator) void {
        if (self.data) |buf| self.c_type.free(ally, buf);
    }
});

pub fn deinitParams(params: *ParamList, allocator: std.mem.Allocator) void {
    for (params.items) |param| {
        param.deinit(allocator);
    }
    params.deinit(allocator);
}

pub fn bindParams(
    stmt: zodbc.Statement,
    py_params: Obj,
    allocator: std.mem.Allocator,
    py_funcs: PyFuncs,
    prepared: *bool,
    query: []const u8,
) !ParamList {
    const seq_or_dict: enum { seq, dict } = if ( //
        c.PySequence_Check(py_params) == 1 //
        and c.PyBytes_Check(py_params) != 1 //
        and c.PyUnicode_Check(py_params) != 1 //
        ) .seq else if (c.PyDict_Check(py_params) == 1) .dict else return py.raise(
            .TypeError,
            "Parameters must be a sequence or dict",
            .{},
        );
    const n_params = std.math.cast(usize, c.PyObject_Length(py_params)) orelse return error.PyErr;

    var params: ParamList = try .initCapacity(allocator, n_params);
    errdefer deinitParams(&params, allocator);

    if (n_params == 0)
        return params;

    const apd = try zodbc.Descriptor.AppParamDesc.fromStatement(stmt);
    const ipd = try zodbc.Descriptor.ImpParamDesc.fromStatement(stmt);

    try apd.setField(0, .count, @intCast(n_params));

    const items_iter = if (seq_or_dict == .dict) blk: {
        const py_items = c.PyObject_CallMethod(py_params, "items", "") orelse return error.PyErr;
        defer c.Py_DECREF(py_items);
        break :blk c.PyObject_GetIter(py_items) orelse return error.PyErr;
    } else null;
    defer if (items_iter) |iter| c.Py_DECREF(iter);

    for (0..n_params) |i_param| {
        const py_val, const py_param_name = if (seq_or_dict == .seq) .{
            c.PySequence_GetItem(py_params, @intCast(i_param)) orelse return error.PyErr,
            null,
        } else blk: {
            const item = c.PyIter_Next(items_iter) orelse return error.PyErr;
            defer c.Py_DECREF(item);
            std.debug.assert(c.PyObject_Length(item) == 2);
            break :blk .{
                c.PySequence_GetItem(item, 1) orelse return error.PyErr,
                c.PySequence_GetItem(item, 0) orelse return error.PyErr,
            };
        };
        defer c.Py_DECREF(py_val);
        defer if (py_param_name) |pn| c.Py_DECREF(pn);

        const is_null = switch (c.Py_IsNone(py_val)) {
            1 => true,
            0 => false,
            else => unreachable,
        };

        if (is_null) {
            try utils.ensurePrepared(stmt, prepared, query, null);

            params.appendAssumeCapacity(.{
                .c_type = undefined,
                .sql_type = undefined,
                .data = null,
                .ind = zodbc.c.SQL_NULL_DATA,
            });

            const TP = @typeInfo(@typeInfo(@TypeOf(zodbc.Statement.describeParam)).@"fn".return_type.?).error_union.payload;
            const desc = stmt.describeParam(@intCast(i_param + 1)) catch TP{
                .length = 0,
                .nullable = .nullable,
                .scale = 0,
                .sql_type = .binary,
            };
            ipd.setField(
                @intCast(i_param + 1),
                .concise_type,
                desc.sql_type,
            ) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, null);

            try ipd.setField(@intCast(i_param + 1), .parameter_type, .input);
            try apd.setField(@intCast(i_param + 1), .indicator_ptr, @ptrCast(&params.items[i_param].ind));

            continue;
        }
        const conv = try Conv.fromValue(py_val, py_funcs);

        params.appendAssumeCapacity(switch (conv) {
            .wchar => .{
                .c_type = .wchar,
                .sql_type = .wvarchar,
                .data = if (is_null) null else blk: {
                    var size: c.Py_ssize_t = -1;
                    const char_ptr = c.PyUnicode_AsUTF8AndSize(
                        py_val,
                        &size,
                    ) orelse return error.PyErr;
                    if (size < 0) {
                        return error.PyErr;
                    }
                    break :blk @ptrCast(try std.unicode.wtf8ToWtf16LeAlloc(
                        allocator,
                        char_ptr[0..@intCast(size)],
                    ));
                },
                .misc = .varsize,
            },
            .binary => .{
                .c_type = .binary,
                .sql_type = .varbinary,
                .data = if (is_null) null else blk: {
                    var ptr: [*c]u8 = null;
                    var size: c.Py_ssize_t = -1;
                    if (c.PyBytes_AsStringAndSize(py_val, &ptr, &size) != 0) return error.PyErr;
                    if (size < 0) {
                        return error.PyErr;
                    }
                    break :blk try allocator.dupe(u8, ptr[0..@intCast(size)]);
                },
                .misc = .varsize,
            },
            .slong => .{
                .c_type = .slong,
                .sql_type = .integer,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .slong,
                    try py.py_to_zig(CDataType.slong.Type(), py_val, null),
                ),
            },
            .sbigint => .{
                .c_type = .sbigint,
                .sql_type = .bigint,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .sbigint,
                    try py.py_to_zig(CDataType.sbigint.Type(), py_val, null),
                ),
            },
            .double => .{
                .c_type = .double,
                .sql_type = .double,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .double,
                    try py.py_to_zig(CDataType.double.Type(), py_val, null),
                ),
            },
            .bit => .{
                .c_type = .bit,
                .sql_type = .bit,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .bit,
                    try py.py_to_zig(CDataType.bit.Type(), py_val, null),
                ),
            },
            .numeric_string => blk: {
                const as_str = c.PyObject_Str(py_val) orelse return error.PyErr;
                defer c.Py_DECREF(as_str);
                var size: c.Py_ssize_t = -1;
                const char_ptr = c.PyUnicode_AsUTF8AndSize(as_str, &size) orelse return error.PyErr;
                if (size < 0) {
                    return error.PyErr;
                }
                const val = try fmt.parseDecimal(char_ptr[0..@intCast(size)]);
                break :blk .{
                    .c_type = .numeric,
                    .sql_type = .numeric,
                    .data = if (is_null) null else try createOdbcVal(
                        allocator,
                        .numeric,
                        val,
                    ),
                    .misc = .{ .dec = .{
                        .precision = val.precision,
                        .scale = val.scale,
                    } },
                };
            },
            .guid => .{
                .c_type = .guid,
                .sql_type = .guid,
                .data = if (is_null) null else blk: {
                    const py_bytes = c.PyObject_GetAttrString(
                        py_val,
                        "bytes_le",
                    ) orelse return error.PyErr;
                    defer c.Py_DECREF(py_bytes);
                    var ptr: [*c]u8 = null;
                    var size: c.Py_ssize_t = -1;
                    if (c.PyBytes_AsStringAndSize(py_bytes, &ptr, &size) != 0) return error.PyErr;
                    if (size < 0) {
                        return error.PyErr;
                    }
                    std.debug.assert(size == 16);
                    break :blk try createOdbcVal(
                        allocator,
                        .guid,
                        @bitCast(ptr[0..16].*),
                    );
                },
            },
            .type_date => .{
                .c_type = .type_date,
                .sql_type = .type_date,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .type_date,
                    try utils.attrsToStruct(zodbc.c.SQL_DATE_STRUCT, py_val),
                ),
                .misc = .{ .dt = .{
                    .strlen = 10,
                    .prec = 0,
                    .isstr = false,
                } },
            },
            .type_time_string => .{
                .c_type = .char,
                .sql_type = .type_time,
                .data = if (is_null) null else blk: {
                    const val = try utils.attrsToStruct(struct {
                        hour: u8,
                        minute: u8,
                        second: u8,
                        microsecond: u32,
                    }, py_val);
                    const str = try allocator.dupe(u8, &fmt.timeToString(
                        6,
                        val.hour,
                        val.minute,
                        val.second,
                        val.microsecond,
                    ));
                    break :blk str;
                },
                .misc = .{ .dt = .{
                    .strlen = @sizeOf(fmt.TimeString(6)),
                    .prec = 6,
                    .isstr = true,
                } },
            },
            .type_timestamp => .{
                .c_type = .type_timestamp,
                .sql_type = .type_timestamp,
                .data = if (is_null) null else blk: {
                    const val = try utils.attrsToStruct(struct {
                        year: u15,
                        month: u8,
                        day: u8,
                        hour: u8,
                        minute: u8,
                        second: u8,
                        microsecond: u32,
                    }, py_val);
                    break :blk try createOdbcVal(allocator, .type_timestamp, .{
                        .year = val.year,
                        .month = val.month,
                        .day = val.day,
                        .hour = val.hour,
                        .minute = val.minute,
                        .second = val.second,
                        .fraction = val.microsecond * 1000,
                    });
                },
                .misc = .{ .dt = .{
                    .strlen = @sizeOf(fmt.DateString) + 1 + @sizeOf(fmt.TimeString(6)),
                    .prec = 6,
                    .isstr = false,
                } },
            },
            .arrow_table => {
                const atvp_type = c.PyObject_GetAttrString(py_val, "_type") orelse return error.PyErr;
                defer c.Py_DECREF(atvp_type);
                const py_table_name = c.PyObject_GetAttrString(atvp_type, "table_name") orelse return error.PyErr;
                defer c.Py_DECREF(py_table_name);
                const table_name = blk: {
                    var sz: isize = 0;
                    const table_name = c.PyUnicode_AsUTF8AndSize(py_table_name, &sz) orelse return error.PyErr;
                    break :blk table_name[0..if (sz < 0) return error.PyErr else @intCast(sz)];
                };

                const schema_caps = c.PyObject_GetAttrString(py_val, "_batch_schema") orelse return error.PyErr;
                defer c.Py_DECREF(schema_caps);
                const array_caps = c.PyObject_GetAttrString(py_val, "_batch_array") orelse return error.PyErr;
                defer c.Py_DECREF(array_caps);
                const schema_batch = try @import("arrow.zig").SchemaCapsule.read_capsule(schema_caps);
                const array_batch = try @import("arrow.zig").ArrayCapsule.read_capsule(array_caps);

                std.debug.assert(schema_batch.n_children == array_batch.n_children);
                const n_cols = array_batch.n_children;
                var n_rows: i64 = array_batch.length;
                try stmt.bindParameter(
                    1,
                    .input,
                    .binary,
                    .ss_table,
                    @intCast(n_cols),
                    0,
                    null,
                    0,
                    @ptrCast(&n_rows),
                );

                std.debug.print("Binding ArrowTVP param {d} with name {s}\n", .{ i_param + 1, table_name });
                const name_16 = try std.unicode.wtf8ToWtf16LeAllocZ(allocator, table_name);
                defer allocator.free(name_16);

                try ipd.setFieldString(@intCast(i_param + 1), .ss_type_name, table_name);
                // try ipd.setFieldString(@intCast(i_param + 1), .ss_schema_name, table_name);
                var idk: [10]i32 = undefined;
                try apd.setField(@intCast(i_param + 1), .data_ptr, @ptrCast(&idk));

                stmt.setStmtAttr(.ss_param_focus, @intCast(i_param + 1)) catch |err| return utils.odbcErrToPy(stmt, "SetStmtAttr", err, null);

                var ind: i64 = 0;
                var data = [_]i32{ 42, 66 };
                apd.setField(@intCast(i_param + 1), .concise_type, .slong) catch return apd.getLastError();
                ipd.setField(@intCast(i_param + 1), .concise_type, .integer) catch return ipd.getLastError();
                apd.setField(@intCast(i_param + 1), .indicator_ptr, @ptrCast(&ind)) catch return apd.getLastError();
                apd.setField(@intCast(i_param + 1), .data_ptr, @ptrCast(&data)) catch return apd.getLastError();
                ipd.setField(@intCast(i_param + 1), .parameter_type, .input) catch return ipd.getLastError();
                stmt.setStmtAttr(.ss_param_focus, 0) catch |err| return utils.odbcErrToPy(stmt, "SetStmtAttr", err, null);

                continue;
            },
        });

        const param = &params.items[i_param];
        if (param.data) |buf| {
            param.ind = @intCast(buf.len);
        }

        // Diff with put_arrow: ind slice vs scalar, no bytes_fixed, data slice vs ptr
        const coln: u15 = @intCast(i_param + 1);
        const thread_state = null;
        apd.setField(coln, .concise_type, param.c_type) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        ipd.setField(coln, .concise_type, param.sql_type) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);

        switch (param.misc) {
            .dt => |info| {
                if (param.c_type == .char) {
                    apd.setField(coln, .length, info.strlen) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                    apd.setField(coln, .octet_length, info.strlen) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                    apd.setField(coln, .octet_length_ptr, @ptrCast(&param.ind)) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                } else {
                    apd.setField(coln, .precision, info.prec) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                    apd.setField(coln, .scale, info.prec) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                }
                ipd.setField(coln, .datetime_interval_precision, info.strlen) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                ipd.setField(coln, .precision, info.prec) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                ipd.setField(coln, .scale, info.prec) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            },
            .dec => |info| {
                ipd.setField(coln, .precision, info.precision) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                ipd.setField(coln, .scale, info.scale) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                apd.setField(coln, .precision, info.precision) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                apd.setField(coln, .scale, info.scale) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            },
            .varsize => {
                ipd.setField(coln, .precision, 0) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                ipd.setField(coln, .length, 0) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
                apd.setField(coln, .octet_length, 0) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                apd.setField(coln, .octet_length_ptr, @ptrCast(&param.ind)) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            },
            .noinfo => {},
            .arrow_tvp => {},
        }

        apd.setField(coln, .indicator_ptr, @ptrCast(&param.ind)) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        if (param.data) |d| {
            apd.setField(coln, .data_ptr, d.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        }
        ipd.setField(coln, .parameter_type, .input) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);

        if (py_param_name) |pn| {
            var len: isize = 0;
            const name = c.PyUnicode_AsUTF8AndSize(pn, &len) orelse return error.PyErr;
            if (len < 0)
                return error.PyErr;
            try ipd.setFieldString(@intCast(i_param + 1), .name, name[0..@intCast(len)]);
        }
    }
    return params;
}
