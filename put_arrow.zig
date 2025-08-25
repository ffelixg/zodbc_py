const std = @import("std");
const zodbc = @import("zodbc");
const arrow = @import("arrow.zig");
const zeit = @import("zeit");
const c = @import("c");
const utils = @import("utils.zig");
const fmt = @import("fmt.zig");
const Param = @import("put_common.zig").Param;
const ParamList = @import("put_common.zig").ParamList;
const DAEInfo = @import("put_common.zig").DAEInfo;
const deinitParams = @import("put_common.zig").deinitParams;

const CDataType = zodbc.odbc.types.CDataType;

inline fn arrowBufferCast(T: type, array: arrow.ArrowArray, is_var: bool) []T {
    return @as([*]T, @alignCast(@ptrCast(array.buffers[1].?)))[0..@intCast(array.length + if (is_var) 1 else 0)];
}

inline fn prepSwitch(str: []const u8) u32 {
    var strarr: [4]u8 = .{ 0, 0, 0, 0 };
    @memcpy(strarr[0..str.len], str);
    return @bitCast(strarr);
}

inline fn fromString(
    format_string: []const u8,
    array: arrow.ArrowArray,
    stmt: zodbc.Statement,
    query: []const u8,
    prepared: *bool,
    thread_state: ?*?*c.PyThreadState,
    i_param: usize,
    ally: std.mem.Allocator,
) !Param {
    if (format_string.len == 0)
        return error.UnrecognizedArrowFormat;
    const ix_sep = std.mem.indexOfScalar(u8, format_string, ':') orelse format_string.len;
    if (ix_sep >= 4) {
        return error.UnrecognizedArrowFormat;
    }

    const ind = try ally.alloc(i64, @intCast(array.length));
    errdefer ally.free(ind);

    if (array.buffers[0]) |valid_buf| {
        const valid: std.DynamicBitSetUnmanaged = .{
            .bit_length = @intCast(array.length),
            .masks = @alignCast(@ptrCast(valid_buf)),
        };
        for (ind, 0..) |*i, ix| {
            i.* = if (valid.isSet(ix)) 0 else zodbc.c.SQL_NULL_DATA;
        }
    } else {
        @memset(ind, 0);
    }

    switch (prepSwitch(format_string[0..ix_sep])) {
        prepSwitch("b") => {
            const buf = try ally.alloc(CDataType.bit.Type(), @intCast(array.length));
            const bits = std.DynamicBitSetUnmanaged{
                .bit_length = @intCast(array.length),
                .masks = @alignCast(@ptrCast(array.buffers[1].?)),
            };
            for (buf, 0..) |*b, ix| {
                b.* = if (bits.isSet(ix)) 1 else 0;
            }
            return Param{
                .c_type = .bit,
                .sql_type = .bit,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .owned,
            };
        },
        inline prepSwitch("c"),
        prepSwitch("C"),
        prepSwitch("s"),
        prepSwitch("S"),
        prepSwitch("i"),
        prepSwitch("I"),
        prepSwitch("l"),
        prepSwitch("L"),
        prepSwitch("f"),
        prepSwitch("g"),
        => |format_comp| {
            const c_type, const sql_type = switch (format_comp) {
                prepSwitch("c") => .{ .stinyint, .tinyint },
                prepSwitch("C") => .{ .utinyint, .tinyint },
                prepSwitch("s") => .{ .sshort, .smallint },
                prepSwitch("S") => .{ .ushort, .smallint },
                prepSwitch("i") => .{ .slong, .integer },
                prepSwitch("I") => .{ .ulong, .integer },
                prepSwitch("l") => .{ .sbigint, .bigint },
                prepSwitch("L") => .{ .ubigint, .bigint },
                prepSwitch("f") => .{ .float, .real },
                prepSwitch("g") => .{ .double, .float },
                else => comptime unreachable,
            };
            return Param{
                .c_type = c_type,
                .sql_type = sql_type,
                .ind = ind,
                .data = array.buffers[1].?[0..@intCast(array.length * comptime zodbc.odbc.types.CDataType.sizeOf(c_type))],
                .ownership = .borrowed,
            };
        },
        // TODO are the 32 bit odbc floats even set up correctly?
        prepSwitch("e") => {
            const arr = arrowBufferCast(f16, array, false);
            const buf = try ally.alloc(CDataType.float.Type(), arr.len);
            for (buf, arr) |*b, v| {
                b.* = v;
            }
            return Param{
                .c_type = .float,
                .sql_type = .real,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .owned,
            };
        },
        inline prepSwitch("z"),
        prepSwitch("Z"),
        prepSwitch("u"),
        prepSwitch("U"),
        => |format_comp| {
            const c_type, const sql_type, const ownership = switch (format_comp) {
                prepSwitch("z") => .{ .binary, .varbinary, .dae_z },
                prepSwitch("Z") => .{ .binary, .varbinary, .dae_Z },
                prepSwitch("u") => .{ .wchar, .wvarchar, .dae_u },
                prepSwitch("U") => .{ .wchar, .wvarchar, .dae_U },
                else => comptime unreachable,
            };
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = zodbc.c.SQL_DATA_AT_EXEC;
                }
            }
            const buf = try ally.create(DAEInfo);
            buf.* = .{
                .i_param = 0,
                .i_col = i_param,
                .i_row = 0,
            };
            return Param{
                .c_type = c_type,
                .sql_type = sql_type,
                .ind = ind,
                // TODO can be done nicer in 0.15
                .data = @ptrCast(@as([*]DAEInfo, @ptrCast(buf))[0..1]),
                .ownership = ownership,
                .misc = .{ .varsize = {} },
            };
        },
        prepSwitch("w") => {
            const width = std.fmt.parseInt(u31, format_string[ix_sep + 1 ..], 10) catch
                return error.UnrecognizedArrowFormat;
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = width;
                }
            }
            return Param{
                .c_type = .binary,
                .sql_type = .binary,
                .ind = ind,
                .data = array.buffers[1].?[0..@intCast(array.length * comptime zodbc.odbc.types.CDataType.sizeOf(.binary))],
                .ownership = .borrowed,
                .misc = .{ .bytes_fixed = width },
            };
        },
        prepSwitch("d") => {
            var parts = std.mem.tokenizeScalar(u8, format_string[ix_sep + 1 ..], ',');
            const precision = parts.next() orelse return error.UnrecognizedArrowFormat;
            const scale = parts.next() orelse return error.UnrecognizedArrowFormat;
            if (parts.next() != null) return error.UnrecognizedArrowFormat;
            const precision_int = std.fmt.parseInt(u8, precision, 10) catch return error.UnrecognizedArrowFormat;
            const scale_int = std.fmt.parseInt(i7, scale, 10) catch return error.UnrecognizedArrowFormat;

            const arr = arrowBufferCast(i128, array, false);
            const buf = try ally.alloc(CDataType.numeric.Type(), arr.len);
            for (buf, arr, ind) |*b, v, i| {
                if (i == zodbc.c.SQL_NULL_DATA)
                    continue;
                b.* = .{
                    .precision = precision_int,
                    .scale = scale_int,
                    .val = @bitCast(if (v < 0) -v else v),
                    .sign = if (v < 0) @as(u1, 0) else @as(u1, 1),
                };
            }
            return Param{
                .c_type = .numeric,
                .sql_type = .numeric,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .owned,
                .misc = .{ .dec = .{ .precision = precision_int, .scale = scale_int } },
            };
        },
        prepSwitch("n") => {
            @memset(ind, zodbc.c.SQL_NULL_DATA);
            try utils.ensurePrepared(stmt, prepared, query, thread_state);
            const desc = stmt.describeParam(@intCast(i_param + 1)) catch |err|
                return utils.odbcErrToPy(stmt, "DescribeParam", err, thread_state);
            return Param{
                .c_type = .default,
                .sql_type = desc.sql_type,
                .ind = ind,
                .data = null,
                .ownership = .borrowed,
            };
        },
        inline prepSwitch("ttu"),
        prepSwitch("ttn"),
        prepSwitch("ttm"),
        => |format_comp| {
            const precision, const A, const trunc_fac = switch (format_comp) {
                prepSwitch("ttm") => .{ 3, u32, 1 },
                prepSwitch("ttu") => .{ 6, u64, 1 },
                prepSwitch("ttn") => .{ 7, u64, 100 },
                else => comptime unreachable,
            };
            const T = fmt.TimeString(precision);
            const fac = std.math.pow(A, 10, precision);
            const arr = arrowBufferCast(A, array, false);
            const data = try ally.alloc(T, @intCast(array.length));
            for (data, arr, ind) |*d, v, *i| {
                if (i.* == zodbc.c.SQL_NULL_DATA)
                    continue;
                i.* = @sizeOf(T);
                d.* = fmt.timeToString(
                    precision,
                    @intCast(@mod(@divFloor(v, trunc_fac * fac * 60 * 60), 24)),
                    @intCast(@mod(@divFloor(v, trunc_fac * fac * 60), 60)),
                    @intCast(@mod(@divFloor(v, trunc_fac * fac), 60)),
                    @intCast(@mod(@divFloor(v, trunc_fac), fac)),
                );
            }
            return Param{
                .c_type = .char,
                .sql_type = .type_time,
                .ind = ind,
                .data = @ptrCast(data),
                .ownership = .owned,
                .misc = .{ .dt = .{ .isstr = true, .prec = precision, .strlen = @sizeOf(T) } },
            };
        },
        inline prepSwitch("tdD"),
        prepSwitch("tdm"),
        prepSwitch("tts"),
        prepSwitch("tss"),
        prepSwitch("tsm"),
        prepSwitch("tsu"),
        prepSwitch("tsn"),
        => |format_comp| {
            const type_enum, const A, const precision, const trunc = switch (comptime format_comp) {
                prepSwitch("tdD") => .{ .type_date, i32, 0, 1 },
                prepSwitch("tdm") => .{ .type_date, i64, 0, 1000 * 3600 * 24 },
                prepSwitch("tts") => .{ .type_time, u32, 0, 1 },
                prepSwitch("tss") => .{ .type_timestamp, i64, 0, 1 },
                prepSwitch("tsm") => .{ .type_timestamp, i64, 3, 1 },
                prepSwitch("tsu") => .{ .type_timestamp, i64, 6, 1 },
                prepSwitch("tsn") => .{ .type_timestamp, i64, 7, 100 },
                else => comptime unreachable,
            };
            const T = @field(CDataType, @tagName(type_enum)).Type();

            comptime var strlen = precision; // fraction
            if (@hasField(T, "year")) strlen += 10; // date
            if (@hasField(T, "year") and @hasField(T, "hour")) strlen += 1; // space between date and time
            if (@hasField(T, "hour")) strlen += 8; // time
            if (@hasField(T, "fraction")) strlen += 1; // period before fraction

            const arr = arrowBufferCast(A, array, false);
            const buf = try ally.alloc(T, @intCast(array.length));
            for (buf, arr) |*b, v| {
                b.* = std.mem.zeroes(T);
                var val = @divFloor(v, trunc);
                if (@hasField(T, "fraction")) {
                    const mod: comptime_int = comptime std.math.pow(i64, 10, precision);
                    const rem: comptime_int = comptime std.math.pow(i64, 10, 9 - precision);
                    b.*.fraction = @intCast(rem * @mod(val, mod));
                    val = @divFloor(val, mod);
                }
                if (@hasField(T, "hour")) {
                    b.*.second = @intCast(@mod(val, 60));
                    val = @divFloor(val, 60);
                    b.*.minute = @intCast(@mod(val, 60));
                    val = @divFloor(val, 60);
                    b.*.hour = @intCast(@mod(val, 24));
                    val = @divFloor(val, 24);
                }
                if (@hasField(T, "year")) {
                    const max_date = comptime zeit.daysFromCivil(.{ .year = 9999, .month = .dec, .day = 31 });
                    const min_date = comptime zeit.daysFromCivil(.{ .year = 1, .month = .jan, .day = 1 });
                    std.debug.assert(val <= max_date);
                    std.debug.assert(val >= min_date);
                    const date = zeit.civilFromDays(@min(@max(val, min_date), max_date));
                    b.*.year = @intCast(date.year);
                    b.*.month = @intFromEnum(date.month);
                    b.*.day = @intCast(date.day);
                } else {
                    std.debug.assert(val == 0);
                }
            }
            return Param{
                .c_type = type_enum,
                .sql_type = type_enum,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .owned,
                .misc = .{ .dt = .{ .isstr = false, .prec = precision, .strlen = strlen } },
            };
        },
        prepSwitch("tDs"),
        prepSwitch("tDm"),
        prepSwitch("tDu"),
        prepSwitch("tDn"),
        => return utils.raise(.NotImplemented, "Durations are not implemented", .{}, thread_state),
        prepSwitch("vz"),
        prepSwitch("vu"),
        => return utils.raise(.NotImplemented, "View-types are not implemented", .{}, thread_state),
        prepSwitch("tiM"),
        prepSwitch("tiD"),
        prepSwitch("tin"),
        => return utils.raise(.NotImplemented, "Intervals are not implemented", .{}, thread_state),
        prepSwitch("+l"),
        prepSwitch("+L"),
        prepSwitch("+vl"),
        prepSwitch("+vL"),
        prepSwitch("+w"),
        prepSwitch("+s"),
        prepSwitch("+m"),
        prepSwitch("+ud"),
        prepSwitch("+us"),
        prepSwitch("+r"),
        => return utils.raise(.NotImplemented, "Complex types are not supported", .{}, thread_state),
        else => return error.UnrecognizedArrowFormat,
    }
}

fn bind(
    i_param: usize,
    param: Param,
    apd: zodbc.Descriptor.AppParamDesc,
    ipd: zodbc.Descriptor.ImpParamDesc,
    thread_state: *?*c.PyThreadState,
) !void {
    const coln: u15 = @intCast(i_param + 1);
    apd.setField(coln, .concise_type, param.c_type) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
    ipd.setField(coln, .concise_type, param.sql_type) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);

    switch (param.misc) {
        .dt => |info| {
            if (param.c_type == .char) {
                apd.setField(coln, .length, info.strlen) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                apd.setField(coln, .octet_length, info.strlen) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                apd.setField(coln, .octet_length_ptr, param.ind.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            } else {
                apd.setField(coln, .precision, info.prec) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
                apd.setField(coln, .scale, info.prec) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            }
            ipd.setField(coln, .datetime_interval_precision, info.strlen) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            ipd.setField(coln, .precision, info.prec) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            ipd.setField(coln, .scale, info.prec) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
        },
        .bytes_fixed => |bytes_fixed_len| {
            apd.setField(coln, .length, bytes_fixed_len) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .octet_length, bytes_fixed_len) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .octet_length_ptr, param.ind.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            ipd.setField(coln, .length, bytes_fixed_len) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
        },
        .dec => |info| {
            ipd.setField(coln, .precision, info.precision) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            ipd.setField(coln, .scale, info.scale) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            apd.setField(coln, .precision, info.precision) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .scale, info.scale) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        },
        .varsize => {
            ipd.setField(coln, .length, 0) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            apd.setField(coln, .octet_length, 0) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .octet_length_ptr, param.ind.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        },
        .arrow_tvp => unreachable, // TODO
        .noinfo => {},
    }

    apd.setField(coln, .indicator_ptr, param.ind.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
    if (param.data) |d| {
        apd.setField(coln, .data_ptr, d.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
    }
    ipd.setField(coln, .parameter_type, .input) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
}

pub fn bindBatch(
    stmt: zodbc.Statement,
    ipd: zodbc.Descriptor.ImpParamDesc,
    apd: zodbc.Descriptor.AppParamDesc,
    query: []const u8,
    prepared: *bool,
    batch_schema: *arrow.ArrowSchema,
    batch_array: *arrow.ArrowArray,
    ally: std.mem.Allocator,
    thread_state: *?*c.PyThreadState,
) !ParamList {
    const n_params: usize = @intCast(batch_array.n_children);
    var param_list: ParamList = try .initCapacity(ally, n_params);
    errdefer deinitParams(&param_list, ally);

    for (0..n_params) |i_param| {
        const fmt_str = std.mem.span(batch_schema.children.?[i_param].*.format);
        const param = fromString(
            fmt_str,
            batch_array.children.?[i_param].*,
            stmt,
            query,
            prepared,
            thread_state,
            i_param,
            ally,
        ) catch |err| switch (err) {
            error.UnrecognizedArrowFormat => return utils.raise(.ValueError, "Unrecognized Arrow format string: {s}", .{fmt_str}, thread_state),
            error.PyErr => return error.PyErr,
            error.OutOfMemory => return error.OutOfMemory,
        };
        param_list.appendAssumeCapacity(param);
        try bind(i_param, param, apd, ipd, thread_state);
    }

    return param_list;
}

pub fn executeMany(
    stmt: zodbc.Statement,
    query: []const u8,
    batch_schema: *arrow.ArrowSchema,
    batch_array: *arrow.ArrowArray,
    ally: std.mem.Allocator,
    thread_state: *?*c.PyThreadState,
) !void {
    var prepared: bool = false;
    const n_params: usize = @intCast(batch_array.n_children);
    std.debug.assert(batch_schema.n_children == n_params);

    const apd = zodbc.Descriptor.AppParamDesc.fromStatement(stmt) catch |err| return utils.odbcErrToPy(stmt, "GetStmtAttr", err, thread_state);
    const ipd = zodbc.Descriptor.ImpParamDesc.fromStatement(stmt) catch |err| return utils.odbcErrToPy(stmt, "GetStmtAttr", err, thread_state);

    apd.setField(0, .count, @intCast(n_params)) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);

    var param_list = try bindBatch(
        stmt,
        ipd,
        apd,
        query,
        &prepared,
        batch_schema,
        batch_array,
        ally,
        thread_state,
    );
    defer deinitParams(&param_list, ally);

    apd.setField(0, .array_size, @intCast(batch_array.length)) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
    var rows_processed: u64 = 0;
    ipd.setField(0, .rows_processed_ptr, &rows_processed) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);

    var need_data: bool = false;
    if (prepared) {
        stmt.execute() catch |err| switch (err) {
            error.ExecuteNoData => {},
            error.ExecuteNeedData => need_data = true,
            else => return utils.odbcErrToPy(stmt, "Execute", err, thread_state),
        };
    } else {
        stmt.execDirect(query) catch |err| switch (err) {
            error.ExecDirectNoData => {},
            error.ExecDirectNeedData => need_data = true,
            else => return utils.odbcErrToPy(stmt, "ExecDirect", err, thread_state),
        };
    }
    if (!need_data) return;
    var u16_buf = try ally.alloc(u16, 4000);
    defer ally.free(u16_buf);
    while (stmt.paramData(DAEInfo) catch |err| {
        return utils.odbcErrToPy(stmt, "ParamData", err, thread_state);
    }) |dae_info| {
        defer dae_info.i_row += 1;
        const param = param_list.items[dae_info.i_col];
        while (param.ind[dae_info.i_row] != zodbc.c.SQL_DATA_AT_EXEC) {
            dae_info.i_row += 1;
            std.debug.assert(dae_info.i_row < batch_array.length);
        }
        std.debug.assert(rows_processed == dae_info.i_row + 1);
        const array = batch_array.children.?[dae_info.i_col].*;
        const data_buf = array.buffers[2].?;

        switch (param.ownership) {
            inline .dae_u, .dae_U, .dae_z, .dae_Z => |dae| {
                const values = arrowBufferCast(switch (dae) {
                    .dae_u, .dae_z => u32,
                    .dae_U, .dae_Z => u64,
                    else => comptime unreachable,
                }, array, true);
                const data = data_buf[values[dae_info.i_row]..values[dae_info.i_row + 1]];
                switch (comptime dae) {
                    .dae_u, .dae_U => {
                        if (data.len >= u16_buf.len) {
                            u16_buf = try ally.realloc(u16_buf, data.len);
                        }
                        const len = std.unicode.wtf8ToWtf16Le(u16_buf, data) catch
                            return utils.raise(
                                .ValueError,
                                "Failed to convert utf8 to utf16le in column {} line {}",
                                .{ dae_info.i_col, dae_info.i_row },
                                thread_state,
                            );
                        stmt.putData(@ptrCast(u16_buf[0..len])) catch |err|
                            return utils.odbcErrToPy(stmt, "PutData", err, thread_state);
                    },
                    .dae_z, .dae_Z => {
                        stmt.putData(data) catch |err|
                            return utils.odbcErrToPy(stmt, "PutData", err, thread_state);
                    },
                    else => comptime unreachable,
                }
            },
            else => unreachable,
        }
    }
}
