const std = @import("std");
const zodbc = @import("zodbc");
const arrow = @import("arrow.zig");
const zeit = @import("zeit");
const c = @import("c");
const utils = @import("utils.zig");

const CDataType = zodbc.odbc.types.CDataType;

const Param = struct {
    c_type: zodbc.odbc.types.CDataType,
    sql_type: zodbc.odbc.types.SQLDataType,
    ind: []i64,
    data: ?[*]u8,
    ownership: enum { owned, borrowed, dae_u, dae_z, dae_U, dae_Z },
    misc: ?union(enum) {
        dt: struct {
            strlen: u7,
            prec: u7,
            isstr: bool,
        },
        dec: struct {
            precision: u8,
            scale: i8,
        },
        bytes_fixed: u31,
        varsize: void,
    } = null,
};

const ParamList = std.ArrayListUnmanaged(Param);

pub fn deinitParams(params: *ParamList, n_params: usize, ally: std.mem.Allocator) void {
    for (params.items) |param| {
        switch (param.ownership) {
            .owned => param.c_type.free(ally, param.data.?[0 .. n_params * param.c_type.sizeOf()]),
            .borrowed => {},
            .dae_u, .dae_z, .dae_U, .dae_Z => ally.destroy(@as(*usize, @ptrCast(@alignCast(param.data.?)))),
        }
        ally.free(param.ind);
    }
    params.deinit(ally);
}

inline fn arrowBufferCast(T: type, array: arrow.ArrowArray, is_var: bool) []T {
    return @as([*]T, @alignCast(@ptrCast(array.buffers[1].?)))[0..@intCast(array.length + if (is_var) 1 else 0)];
}

inline fn arrowValid(array: arrow.ArrowArray) ?std.DynamicBitSetUnmanaged {
    if (array.buffers[0]) |valid_buf| {
        return .{
            .bit_length = @intCast(array.length),
            .masks = @alignCast(@ptrCast(valid_buf)),
        };
    } else return null;
}

inline fn dtBuf(
    A: type,
    array: arrow.ArrowArray,
    comptime c_type: CDataType,
    prec: anytype,
    ally: std.mem.Allocator,
) ![*]u8 {
    const T = c_type.Type();
    const arr = arrowBufferCast(A, array, false);
    const buf = try ally.alloc(T, @intCast(array.length));
    for (buf, arr) |*b, v| {
        b.* = std.mem.zeroes(T);
        var val = v;
        if (@hasField(T, "fraction")) {
            switch (prec) {
                0 => {},
                3 => {
                    b.*.fraction = @intCast(1_000_000 * @mod(val, 1_000));
                    val = @divFloor(val, 1_000);
                },
                6 => {
                    b.*.fraction = @intCast(1_000 * @mod(val, 1_000_000));
                    val = @divFloor(val, 1_000_000);
                },
                9 => {
                    b.*.fraction = @intCast(@mod(val, 1_000_000_000));
                    val = @divFloor(val, 1_000_000_000);
                },
                else => comptime unreachable,
            }
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
            // Min => silent failure but also allows skipping null check
            const max_date = comptime zeit.daysFromCivil(.{ .year = 9999, .month = .dec, .day = 31 });
            const date = zeit.civilFromDays(@min(val, max_date));
            b.*.year = @intCast(date.year);
            b.*.month = @intFromEnum(date.month);
            b.*.day = @intCast(date.day);
        } else {
            std.debug.assert(val == 0);
        }
    }
    return @ptrCast(buf.ptr);
}

fn prepSwitch(str: []const u8) u32 {
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
    if (prepSwitch(format_string[0..ix_sep]) == prepSwitch("n")) {
        @memset(ind, zodbc.c.SQL_NULL_DATA);
    } else if (arrowValid(array)) |valid| {
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
                .data = @ptrCast(buf.ptr),
                .ownership = .owned,
            };
        },
        prepSwitch("c") => return Param{
            .c_type = .stinyint,
            .sql_type = .tinyint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("C") => return Param{
            .c_type = .utinyint,
            .sql_type = .tinyint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("s") => return Param{
            .c_type = .sshort,
            .sql_type = .smallint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("S") => return Param{
            .c_type = .ushort,
            .sql_type = .smallint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("i") => return Param{
            .c_type = .slong,
            .sql_type = .integer,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("I") => return Param{
            .c_type = .ulong,
            .sql_type = .integer,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("l") => return Param{
            .c_type = .sbigint,
            .sql_type = .bigint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("L") => return Param{
            .c_type = .ubigint,
            .sql_type = .bigint,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
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
                .data = @ptrCast(buf.ptr),
                .ownership = .owned,
            };
        },
        prepSwitch("f") => return Param{
            .c_type = .float,
            .sql_type = .real,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("g") => return Param{
            .c_type = .double,
            .sql_type = .float,
            .ind = ind,
            .data = array.buffers[1],
            .ownership = .borrowed,
        },
        prepSwitch("z") => {
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = zodbc.c.SQL_DATA_AT_EXEC;
                }
            }
            const buf = try ally.create(usize);
            buf.* = i_param;
            return Param{
                .c_type = .binary,
                .sql_type = .varbinary,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .dae_z,
                .misc = .{ .varsize = {} },
            };
        },
        prepSwitch("Z") => {
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = zodbc.c.SQL_DATA_AT_EXEC;
                }
            }
            const buf = try ally.create(usize);
            buf.* = i_param;
            return Param{
                .c_type = .binary,
                .sql_type = .varbinary,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .dae_Z,
                .misc = .{ .varsize = {} },
            };
        },
        prepSwitch("u") => {
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = zodbc.c.SQL_DATA_AT_EXEC;
                }
            }
            const buf = try ally.create(usize);
            buf.* = i_param;
            return Param{
                .c_type = .wchar,
                .sql_type = .wvarchar,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .dae_u,
                .misc = .{ .varsize = {} },
            };
        },
        prepSwitch("U") => {
            for (ind) |*i| {
                if (i.* == 0) {
                    i.* = zodbc.c.SQL_DATA_AT_EXEC;
                }
            }
            const buf = try ally.create(usize);
            buf.* = i_param;
            return Param{
                .c_type = .wchar,
                .sql_type = .wvarchar,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = .dae_U,
                .misc = .{ .varsize = {} },
            };
        },
        prepSwitch("w") => {
            return Param{
                .c_type = .binary,
                .sql_type = .binary,
                .ind = ind,
                .data = array.buffers[1],
                .ownership = .borrowed,
                .misc = .{ .bytes_fixed = std.fmt.parseInt(
                    u31,
                    format_string[ix_sep + 1 ..],
                    10,
                ) catch return error.UnrecognizedArrowFormatInfo },
            };
        },
        prepSwitch("d") => {
            var parts = std.mem.tokenizeScalar(u8, format_string[ix_sep + 1 ..], ',');
            const precision = parts.next() orelse return error.UnrecognizedArrowFormatInfo;
            const scale = parts.next() orelse return error.UnrecognizedArrowFormatInfo;
            if (parts.next() != null) return error.UnrecognizedArrowFormatInfo;
            const precision_int = std.fmt.parseInt(u8, precision, 10) catch return error.UnrecognizedArrowFormatInfo;
            const scale_int = std.fmt.parseInt(i7, scale, 10) catch return error.UnrecognizedArrowFormatInfo;

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
                .data = @ptrCast(buf.ptr),
                .ownership = .owned,
                .misc = .{ .dec = .{ .precision = precision_int, .scale = scale_int } },
            };
        },
        prepSwitch("n") => {
            try utils.ensurePrepared(stmt, prepared, query, thread_state);
            const desc = stmt.describeParam(@intCast(i_param + 1)) catch |err| return utils.odbcErrToPy(
                stmt,
                "DescribeParam",
                err,
                thread_state,
            );
            return Param{
                .c_type = .default,
                .sql_type = desc.sql_type,
                .ind = ind,
                .data = null,
                .ownership = .borrowed,
            };
        },
        prepSwitch("tdD") => return Param{
            .c_type = .type_date,
            .sql_type = .type_date,
            .ind = ind,
            .data = try dtBuf(i32, array, .type_date, 0, ally),
            .ownership = .owned,
            .misc = .{ .dt = .{ .isstr = false, .prec = 0, .strlen = 10 } },
        },
        // prepSwitch("tdm") => .date64_milliseconds,
        prepSwitch("tts") => return Param{
            .c_type = .type_time,
            .sql_type = .type_time,
            .ind = ind,
            .data = try dtBuf(i32, array, .type_time, 0, ally),
            .ownership = .owned,
            .misc = .{ .dt = .{ .isstr = false, .prec = 0, .strlen = 8 } },
        },
        // prepSwitch("ttm") => .time32_milliseconds,
        // prepSwitch("ttu") => .time64_microseconds,
        // prepSwitch("ttn") => .time64_nanoseconds,
        prepSwitch("tss") => return Param{
            .c_type = .type_timestamp,
            .sql_type = .type_timestamp,
            .ind = ind,
            .data = try dtBuf(i64, array, .type_timestamp, 0, ally),
            .ownership = .owned,
            .misc = .{ .dt = .{ .isstr = false, .prec = 0, .strlen = 19 } },
        },
        prepSwitch("tsm") => return Param{
            .c_type = .type_timestamp,
            .sql_type = .type_timestamp,
            .ind = ind,
            .data = try dtBuf(i64, array, .type_timestamp, 3, ally),
            .ownership = .owned,
            .misc = .{ .dt = .{ .isstr = false, .prec = 3, .strlen = 23 } },
        },
        prepSwitch("tsu") => return Param{
            .c_type = .type_timestamp,
            .sql_type = .type_timestamp,
            .ind = ind,
            .data = try dtBuf(i64, array, .type_timestamp, 6, ally),
            .ownership = .owned,
            .misc = .{ .dt = .{ .isstr = false, .prec = 6, .strlen = 26 } },
        },
        prepSwitch("tsn") => return Param{
            .c_type = .type_timestamp,
            .sql_type = .type_timestamp,
            .ind = ind,
            .data = try dtBuf(i64, array, .type_timestamp, 9, ally),
            .ownership = .owned,
            // TODO ok to just truncate to 7?
            // Maybe this should be driver dependent or look at the parameter description
            .misc = .{ .dt = .{ .isstr = false, .prec = 7, .strlen = 27 } },
        },
        prepSwitch("tDs"),
        prepSwitch("tDm"),
        prepSwitch("tDu"),
        prepSwitch("tDn"),
        => return error.DurationsNotImplemented,
        prepSwitch("vz"),
        prepSwitch("vu"),
        => return error.ViewtypesNotImplemented,
        prepSwitch("tiM"),
        prepSwitch("tiD"),
        prepSwitch("tin"),
        => return error.IntervalsNotImplemented,
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
        => return error.ComplexTypesNotSupported,
        else => return error.UnrecognizedArrowFormat,
    }
}

fn bind(
    i_param: usize,
    param: Param,
    apd: zodbc.Descriptor.AppParamDesc,
    ipd: zodbc.Descriptor.ImpParamDesc,
) !void {
    const coln: u15 = @intCast(i_param + 1);
    try apd.setField(coln, .concise_type, param.c_type);
    try ipd.setField(coln, .concise_type, param.sql_type);

    if (param.misc) |misc| {
        switch (misc) {
            .dt => |info| {
                if (param.c_type == .char) {
                    try apd.setField(coln, .length, info.strlen);
                    try apd.setField(coln, .octet_length, info.strlen);
                } else {
                    try apd.setField(coln, .precision, info.prec);
                    try apd.setField(coln, .scale, info.prec);
                }
                try ipd.setField(coln, .datetime_interval_precision, info.strlen);
                try ipd.setField(coln, .precision, info.prec);
                try ipd.setField(coln, .scale, info.prec);
            },
            .bytes_fixed => |bytes_fixed_len| {
                try apd.setField(coln, .octet_length, bytes_fixed_len);
                try ipd.setField(coln, .length, bytes_fixed_len);
            },
            .dec => |info| {
                try ipd.setField(coln, .precision, info.precision);
                try ipd.setField(coln, .scale, info.scale);
                try apd.setField(coln, .precision, info.precision);
                try apd.setField(coln, .scale, info.scale);
            },
            .varsize => {
                try apd.setField(coln, .octet_length, 0);
                try ipd.setField(coln, .length, 0);
                try apd.setField(coln, .octet_length_ptr, param.ind.ptr);
            },
        }
    }

    try apd.setField(coln, .indicator_ptr, param.ind.ptr);
    if (param.data) |d| {
        try apd.setField(coln, .data_ptr, d);
    }
    try ipd.setField(coln, .parameter_type, .input);
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

    var param_list: ParamList = try .initCapacity(ally, n_params);
    defer deinitParams(&param_list, @intCast(batch_array.length), ally);

    const apd = try zodbc.Descriptor.AppParamDesc.fromStatement(stmt);
    const ipd = try zodbc.Descriptor.ImpParamDesc.fromStatement(stmt);

    try apd.setField(0, .count, @intCast(n_params));

    for (0..n_params) |i_param| {
        const param = try fromString(
            std.mem.span(batch_schema.children.?[i_param].*.format),
            batch_array.children.?[i_param].*,
            stmt,
            query,
            &prepared,
            thread_state,
            i_param,
            ally,
        );
        param_list.appendAssumeCapacity(param);
        try bind(i_param, param, apd, ipd);
    }

    try apd.setField(0, .array_size, @intCast(batch_array.length));
    var rows_processed: u64 = 0;
    try ipd.setField(0, .rows_processed_ptr, &rows_processed);

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
    while (stmt.paramData(usize) catch |err| {
        return utils.odbcErrToPy(stmt, "ParamData", err, thread_state);
    }) |i_param| {
        const param = param_list.items[i_param.*];
        const array = batch_array.children.?[i_param.*].*;
        const data_buf = array.buffers[2].?;

        switch (param.ownership) {
            inline .dae_u, .dae_U, .dae_z, .dae_Z => |dae| {
                const values = arrowBufferCast(switch (dae) {
                    .dae_u => u32,
                    .dae_U => u64,
                    .dae_z => u32,
                    .dae_Z => u64,
                    else => comptime unreachable,
                }, array, true);
                const data = data_buf[values[rows_processed - 1]..values[rows_processed]];
                switch (comptime dae) {
                    .dae_u, .dae_U => {
                        if (data.len >= u16_buf.len) {
                            u16_buf = try ally.realloc(u16_buf, data.len);
                        }
                        const len = try std.unicode.wtf8ToWtf16Le(u16_buf, data);
                        stmt.putData(@ptrCast(u16_buf[0..len])) catch |err| {
                            return utils.odbcErrToPy(stmt, "PutData", err, thread_state);
                        };
                    },
                    .dae_z, .dae_Z => {
                        stmt.putData(data) catch |err| {
                            return utils.odbcErrToPy(stmt, "PutData", err, thread_state);
                        };
                    },
                    else => comptime unreachable,
                }
            },
            else => unreachable,
        }
    }
}
