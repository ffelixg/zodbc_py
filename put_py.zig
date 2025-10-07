const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;
const PyFuncs = @import("PyFuncs.zig");
const CDataType = zodbc.odbc.types.CDataType;
const utils = @import("utils.zig");
const fmt = @import("fmt.zig");
const arrow = @import("arrow.zig");
const put_arrow = @import("put_arrow.zig");
const Param = @import("put_common.zig").Param;
const ParamList = @import("put_common.zig").ParamList;
const DAEInfo = @import("put_common.zig").DAEInfo;
const deinitParams = @import("put_common.zig").deinitParams;
const bindList = @import("put_common.zig").bindList;

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
        } else if (blk: {
            // if (1 == c.PyObject_IsInstance(val, funcs.cls_arrow_table)) {
            const val_type = c.PyObject_Type(val) orelse return error.PyErr;
            defer c.Py_DECREF(val_type);
            const type_name = c.PyObject_GetAttrString(val_type, "__name__") orelse return error.PyErr;
            defer c.Py_DECREF(type_name);
            var sz: isize = 0;
            const name = c.PyUnicode_AsUTF8AndSize(type_name, &sz) orelse return error.PyErr;
            if (sz < 0) return error.PyErr;
            break :blk std.mem.eql(u8, name[0..@intCast(sz)], "ArrowTVP");
        }) {
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

pub fn bindParams(
    stmt: zodbc.Statement,
    py_params: Obj,
    allocator: std.mem.Allocator,
    py_funcs: PyFuncs,
    prepared: *bool,
    query: []const u8,
) !ParamList {
    var thread_state_inner: ?*c.PyThreadState = null;
    const thread_state = &thread_state_inner;

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

        const ind = try allocator.alloc(i64, 1);
        errdefer allocator.free(ind);
        ind[0] = zodbc.c.SQL_NULL_DATA;

        if (is_null) {
            // try utils.ensurePrepared(stmt, prepared, query, null);
            // const TP = @typeInfo(@typeInfo(@TypeOf(zodbc.Statement.describeParam)).@"fn".return_type.?).error_union.payload;
            // const desc = stmt.describeParam(@intCast(i_param + 1)) catch TP{
            //     .length = 0,
            //     .nullable = .nullable,
            //     .scale = 0,
            //     .sql_type = .binary,
            // };

            // params.appendAssumeCapacity(.{
            //     .c_type = .default,
            //     .sql_type = desc.sql_type,
            //     .ind = ind,
            //     .data = null,
            //     .misc = .null,
            // });

            params.appendAssumeCapacity(.{
                .c_type = .default,
                .sql_type = .wvarchar,
                .ind = ind,
                .data = null,
                .misc = .varsize,
            });

            continue;
        }
        const conv = try Conv.fromValue(py_val, py_funcs);

        params.appendAssumeCapacity(switch (conv) {
            .wchar => .{
                .c_type = .wchar,
                .sql_type = .wvarchar,
                .ind = ind,
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
                .ind = ind,
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
                .ind = ind,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .slong,
                    try py.py_to_zig(CDataType.slong.Type(), py_val, null),
                ),
            },
            .sbigint => .{
                .c_type = .sbigint,
                .sql_type = .bigint,
                .ind = ind,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .sbigint,
                    try py.py_to_zig(CDataType.sbigint.Type(), py_val, null),
                ),
            },
            .double => .{
                .c_type = .double,
                .sql_type = .double,
                .ind = ind,
                .data = if (is_null) null else try createOdbcVal(
                    allocator,
                    .double,
                    try py.py_to_zig(CDataType.double.Type(), py_val, null),
                ),
            },
            .bit => .{
                .c_type = .bit,
                .sql_type = .bit,
                .ind = ind,
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
                    .ind = ind,
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
                .ind = ind,
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
                .ind = ind,
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
                .ind = ind,
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
                .ind = ind,
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
            .arrow_table => blk: {
                const atvp_type = c.PyObject_GetAttrString(py_val, "_type") orelse return error.PyErr;
                defer c.Py_DECREF(atvp_type);

                const table_name = blki: {
                    const py_name = c.PyObject_GetAttrString(atvp_type, "table_name") orelse return error.PyErr;
                    defer c.Py_DECREF(py_name);
                    var sz: isize = 0;
                    const name = c.PyUnicode_AsUTF8AndSize(py_name, &sz) orelse return error.PyErr;
                    break :blki try allocator.dupe(u8, name[0..if (sz < 0) return error.PyErr else @intCast(sz)]);
                };
                errdefer allocator.free(table_name);

                const schema_name: ?[]u8 = blki: {
                    const py_name = c.PyObject_GetAttrString(atvp_type, "schema_name") orelse return error.PyErr;
                    defer c.Py_DECREF(py_name);
                    if (c.Py_IsNone(py_name) == 1) {
                        break :blki null;
                    }
                    var sz: isize = 0;
                    const name = c.PyUnicode_AsUTF8AndSize(py_name, &sz) orelse return error.PyErr;
                    break :blki try allocator.dupe(u8, name[0..if (sz < 0) return error.PyErr else @intCast(sz)]);
                };
                errdefer if (schema_name) |s| allocator.free(s);

                const schema_caps = c.PyObject_GetAttrString(py_val, "_batch_schema") orelse return error.PyErr;
                errdefer c.Py_DECREF(schema_caps);
                const array_caps = c.PyObject_GetAttrString(py_val, "_batch_array") orelse return error.PyErr;
                errdefer c.Py_DECREF(array_caps);
                const batch_schema = try arrow.SchemaCapsule.read_capsule(schema_caps);
                const batch_array = try arrow.ArrayCapsule.read_capsule(array_caps);

                std.debug.assert(batch_schema.n_children == batch_array.n_children);
                ind[0] = batch_array.length;

                break :blk .{
                    .c_type = .binary,
                    .sql_type = .ss_table,
                    .ind = ind,
                    .data = null,
                    .misc = .{ .arrow_tvp = .{
                        .schema_caps = schema_caps,
                        .array_caps = array_caps,
                        .batch_schema = batch_schema,
                        .batch_array = batch_array,
                        .schema_name = schema_name,
                        .table_name = table_name,
                        .param_list = try put_arrow.batchToParams(
                            stmt,
                            query,
                            prepared,
                            batch_schema,
                            batch_array,
                            allocator,
                            thread_state,
                        ),
                    } },
                };
            },
        });

        const param = &params.items[i_param];
        if (param.data) |buf| {
            param.ind[0] = @intCast(buf.len);
        }

        if (py_param_name) |pn| {
            var len: isize = 0;
            const name = c.PyUnicode_AsUTF8AndSize(pn, &len) orelse return error.PyErr;
            if (len < 0)
                return error.PyErr;
            param.name = try allocator.dupe(u8, name[0..@intCast(len)]);
        }
    }

    const apd = try zodbc.Descriptor.AppParamDesc.fromStatement(stmt);
    const ipd = try zodbc.Descriptor.ImpParamDesc.fromStatement(stmt);

    try bindList(
        stmt,
        ipd,
        apd,
        params,
        thread_state,
    );

    return params;
}
