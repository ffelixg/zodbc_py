const std = @import("std");
const zodbc = @import("zodbc");
const arrow = @import("arrow.zig");
const c = @import("c");
const Obj = *c.PyObject;

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
    misc: union(enum) {
        noinfo: void,
        varsize: void,
        dt: struct { strlen: u7, prec: u7, isstr: bool },
        dec: struct { precision: u8, scale: i8 },
        bytes_fixed: u31,
        arrow_tvp: struct {
            schema_caps: Obj,
            array_caps: Obj,
            batch_schema: *arrow.ArrowSchema,
            batch_array: *arrow.ArrowArray,
            schema_name: ?[]u8,
            table_name: []u8,
            param_list: ?ParamList,
        },
    } = .noinfo,

    pub fn deinit(param: *Param, ally: std.mem.Allocator) void {
        ally.free(param.ind);
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
                if (arrow_tvp.param_list) |*pl| {
                    deinitParams(pl, ally);
                }
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
