const std = @import("std");
const zodbc = @import("zodbc");
const arrow = @import("arrow.zig");
const c = @import("c");
const Obj = *c.PyObject;
const utils = @import("utils.zig");

pub const DAEInfo = struct {
    /// 0 for executemany, number of column for TVPs
    i_param: usize,

    i_col: usize,
    i_row: usize,
};

pub const Param = struct {
    c_type: zodbc.odbc.types.CDataType,
    sql_type: zodbc.odbc.types.SQLDataType,
    ind: []i64,
    data: ?[]u8,
    ownership: enum { owned, borrowed, dae_u, dae_z, dae_U, dae_Z } = .owned,
    name: ?[]u8 = null,
    misc: union(enum) {
        noinfo: void,
        varsize: void,
        dt: struct { strlen: u7, prec: u7, isstr: bool },
        dec: struct { precision: u8, scale: i8 },
        bytes_fixed: u31,
        null: void,
        arrow_tvp: struct {
            schema_caps: Obj,
            array_caps: Obj,
            batch_schema: *arrow.ArrowSchema,
            batch_array: *arrow.ArrowArray,
            schema_name: ?[]u8,
            table_name: []u8,
            param_list: ParamList,
        },
    } = .noinfo,

    pub fn deinit(param: *Param, ally: std.mem.Allocator) void {
        ally.free(param.ind);
        if (param.name) |name| {
            ally.free(name);
        }
        if (param.data) |data| {
            switch (param.ownership) {
                .owned => param.c_type.free(ally, data),
                .borrowed => {},
                .dae_u, .dae_z, .dae_U, .dae_Z => ally.destroy(@as(*DAEInfo, @ptrCast(@alignCast(data)))),
            }
        }
        switch (param.misc) {
            .arrow_tvp => |*arrow_tvp| {
                c.Py_DECREF(arrow_tvp.schema_caps);
                c.Py_DECREF(arrow_tvp.array_caps);
                if (arrow_tvp.schema_name) |s| {
                    ally.free(s);
                }
                ally.free(arrow_tvp.table_name);
                deinitParams(&arrow_tvp.param_list, ally);
            },
            else => {},
        }
    }
};

pub const ParamList = std.ArrayListUnmanaged(Param);

pub fn deinitParams(params: *ParamList, allocator: std.mem.Allocator) void {
    for (params.items) |*param| {
        param.deinit(allocator);
    }
    params.deinit(allocator);
}

fn bindSingle(
    i_param: usize,
    stmt: zodbc.Statement,
    ipd: zodbc.Descriptor.ImpParamDesc,
    apd: zodbc.Descriptor.AppParamDesc,
    param: Param,
    thread_state: *?*c.PyThreadState,
) error{PyErr}!void {
    const coln: u15 = @intCast(i_param + 1);
    const ipd_length: u64, const decimal_digits, const apd_length: i64 = switch (param.misc) {
        .dt => |info| .{
            info.strlen,
            info.prec,
            if (param.c_type == .char) info.strlen else info.prec,
        },
        .dec => |info| .{
            info.precision,
            info.scale,
            0, // set later
        },
        .varsize => .{ 0, 0, 0 },
        .noinfo => .{ 0, 0, 0 },
        .null => .{ 1, 0, 1 },
        .bytes_fixed => |len| .{ @intCast(len), 0, len },
        .arrow_tvp => |info| .{ @intCast(info.batch_array.n_children), 0, 0 },
    };

    stmt.bindParameter(
        coln,
        .input,
        param.c_type,
        param.sql_type,
        ipd_length,
        decimal_digits,
        if (param.data) |d| d.ptr else null,
        apd_length,
        param.ind.ptr,
    ) catch |err| return utils.odbcErrToPy(stmt, "BindParameter", err, thread_state);

    switch (param.misc) {
        .dec => |info| {
            // setting concise type resets some apd related values I think
            apd.setField(coln, .concise_type, param.c_type) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .precision, info.precision) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .scale, info.scale) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
            apd.setField(coln, .data_ptr, param.data.?.ptr) catch |err| return utils.odbcErrToPy(apd, "SetDescField", err, thread_state);
        },
        .arrow_tvp => |*info| {
            ipd.setFieldString(coln, .ss_type_name, info.table_name) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            if (info.schema_name) |s| {
                ipd.setFieldString(coln, .ss_schema_name, s) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
            }

            stmt.setStmtAttr(.ss_param_focus, coln) catch |err| return utils.odbcErrToPy(stmt, "SetStmtAttr", err, thread_state);
            errdefer stmt.setStmtAttr(.ss_param_focus, 0) catch {};

            try bindList(
                stmt,
                ipd,
                apd,
                info.param_list,
                thread_state,
            );

            stmt.setStmtAttr(.ss_param_focus, 0) catch |err| return utils.odbcErrToPy(stmt, "SetStmtAttr", err, thread_state);
        },
        else => {},
    }

    if (param.name) |name| {
        ipd.setFieldString(coln, .name, name) catch |err| return utils.odbcErrToPy(ipd, "SetDescField", err, thread_state);
    }
}

pub fn bindList(
    stmt: zodbc.Statement,
    ipd: zodbc.Descriptor.ImpParamDesc,
    apd: zodbc.Descriptor.AppParamDesc,
    param_list: ParamList,
    thread_state: *?*c.PyThreadState,
) error{PyErr}!void {
    for (param_list.items, 0..) |param, i_param| {
        try bindSingle(
            i_param,
            stmt,
            ipd,
            apd,
            param,
            thread_state,
        );
    }
}
