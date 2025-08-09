const std = @import("std");
const zodbc = @import("zodbc");
const arrow = @import("arrow.zig");
const zeit = @import("zeit");
const c = @import("c");
const utils = @import("utils.zig");
const fmt = @import("fmt.zig");

const CDataType = zodbc.odbc.types.CDataType;

const Param = struct {
    c_type: zodbc.odbc.types.CDataType,
    sql_type: zodbc.odbc.types.SQLDataType,
    ind: []i64,
    data: ?[*]u8,
    ownership: enum { owned, borrowed, dae_u, dae_z, dae_U, dae_Z },
    misc: union(enum) {
        dt: struct { strlen: u7, prec: u7, isstr: bool },
        dec: struct { precision: u8, scale: i8 },
        bytes_fixed: u31,
        varsize: void,
        noinfo: void,
    } = .noinfo,
};

const ParamList = std.ArrayListUnmanaged(Param);

pub fn deinitParams(params: *ParamList, len: usize, ally: std.mem.Allocator) void {
    for (params.items) |param| {
        switch (param.ownership) {
            .owned => {
                if (param.misc == .dt and param.misc.dt.isstr) {
                    ally.free(param.data.?[0 .. len * param.misc.dt.strlen]);
                } else {
                    param.c_type.free(ally, param.data.?[0 .. len * param.c_type.sizeOf()]);
                }
            },
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
                .data = @ptrCast(buf.ptr),
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
                .data = array.buffers[1],
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
                .data = @ptrCast(buf.ptr),
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
            const buf = try ally.create(usize);
            buf.* = i_param;
            return Param{
                .c_type = c_type,
                .sql_type = sql_type,
                .ind = ind,
                .data = @ptrCast(buf),
                .ownership = ownership,
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
                ) catch return error.UnrecognizedArrowFormat },
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
                .data = @ptrCast(buf.ptr),
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
                .data = @ptrCast(data.ptr),
                .ownership = .owned,
                .misc = .{ .dt = .{ .isstr = true, .prec = precision, .strlen = @sizeOf(T) } },
            };
        },
        inline prepSwitch("tdD"),
        prepSwitch("tts"),
        prepSwitch("tss"),
        prepSwitch("tsm"),
        prepSwitch("tsu"),
        prepSwitch("tsn"),
        => |format_comp| {
            const type_enum, const A, const precision, const trunc = switch (comptime format_comp) {
                prepSwitch("tdD") => .{ .type_date, u32, 0, 1 },
                prepSwitch("tts") => .{ .type_time, u32, 0, 1 },
                prepSwitch("tss") => .{ .type_timestamp, u64, 0, 1 },
                prepSwitch("tsm") => .{ .type_timestamp, u64, 3, 1 },
                prepSwitch("tsu") => .{ .type_timestamp, u64, 6, 1 },
                prepSwitch("tsn") => .{ .type_timestamp, u64, 7, 100 },
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
                    std.debug.assert(val <= max_date);
                    const date = zeit.civilFromDays(@min(val, max_date));
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
                .data = @ptrCast(buf.ptr),
                .ownership = .owned,
                .misc = .{ .dt = .{ .isstr = false, .prec = precision, .strlen = strlen } },
            };
        },
        prepSwitch("tdm") => return error.Date64NotImplemented,
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

    switch (param.misc) {
        .dt => |info| {
            if (param.c_type == .char) {
                try apd.setField(coln, .length, info.strlen);
                try apd.setField(coln, .octet_length, info.strlen);
                try apd.setField(coln, .octet_length_ptr, param.ind.ptr);
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
            try ipd.setField(coln, .length, 0);
            try apd.setField(coln, .octet_length, 0);
            try apd.setField(coln, .octet_length_ptr, param.ind.ptr);
        },
        .noinfo => {},
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
    defer ally.free(u16_buf);
    while (stmt.paramData(usize) catch |err| {
        return utils.odbcErrToPy(stmt, "ParamData", err, thread_state);
    }) |i_param| {
        const param = param_list.items[i_param.*];
        const array = batch_array.children.?[i_param.*].*;
        const data_buf = array.buffers[2].?;

        switch (param.ownership) {
            inline .dae_u, .dae_U, .dae_z, .dae_Z => |dae| {
                const values = arrowBufferCast(switch (dae) {
                    .dae_u, .dae_z => u32,
                    .dae_U, .dae_Z => u64,
                    else => comptime unreachable,
                }, array, true);
                const data = data_buf[values[rows_processed - 1]..values[rows_processed]];
                switch (comptime dae) {
                    .dae_u, .dae_U => {
                        if (data.len >= u16_buf.len) {
                            u16_buf = try ally.realloc(u16_buf, data.len);
                        }
                        const len = try std.unicode.wtf8ToWtf16Le(u16_buf, data);
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
