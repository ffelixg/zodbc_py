const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const utils = @import("utils.zig");
const PyFuncs = @import("PyFuncs.zig");
const FetchPy = @import("fetch_py.zig");
const put_py = @import("put_py.zig");
const fetch_py = FetchPy.fetch_py;
const FetchArrow = @import("fetch_arrow.zig");
const arrow = @import("arrow.zig");
const c = py.py;
const Obj = *c.PyObject;

const PyErr = py.PyErr;

const EnvCon = struct {
    env: zodbc.Environment,
    con: zodbc.Connection,
    py_funcs: PyFuncs,
    closed: bool = false,

    fn close(self: *EnvCon) !void {
        if (self.closed) return;
        self.closed = true;
        self.con.endTran(.rollback) catch return self.con.getLastError();
        self.con.disconnect() catch return self.con.getLastError();
    }

    /// Only called via garbage collection
    fn deinit(self: *EnvCon) callconv(.c) void {
        self.close() catch {};
        self.py_funcs.deinit();
        // TODO maybe use python warnings?
        self.con.deinit() catch {};
        self.env.deinit() catch {};
    }
};
const ConnectionCapsule = py.PyCapsule(EnvCon, "zodbc_con", EnvCon.deinit);

const Stmt = struct {
    stmt: zodbc.Statement,
    /// Keep a reference to the connection capsule so it isn't
    /// accidentally garbage collected before all its statements
    env_con_caps: Obj,
    /// Borrowed reference
    env_con: *const EnvCon,
    dt7_fetch: utils.Dt7Fetch,
    rowcount: i64 = -1,

    result_set: ?struct {
        result_set: zodbc.ResultSet,
        stmt: *const Stmt,
        cache_column_names: ?std.ArrayListUnmanaged([:0]const u8) = null,
        cache_tuple_type: ?*c.PyTypeObject = null,
        cache_fetch_py_state: ?FetchPy = null,
        cache_fetch_arrow_state: ?FetchArrow = null,

        fn init(stmt: *const Stmt, allocator: std.mem.Allocator) !@This() {
            const thread_state = c.PyEval_SaveThread();
            defer c.PyEval_RestoreThread(thread_state);
            return .{
                .result_set = try .init(
                    stmt.stmt,
                    allocator,
                ),
                .stmt = stmt,
            };
        }

        fn deinit(self: *@This()) !void {
            if (self.cache_column_names) |*names| {
                for (names.items) |name| {
                    std.heap.smp_allocator.free(name);
                }
                names.deinit(std.heap.smp_allocator);
            }
            if (self.cache_tuple_type) |tp| {
                c.Py_DECREF(@alignCast(@ptrCast(tp)));
            }
            if (self.cache_fetch_py_state) |fp| {
                fp.deinit(std.heap.smp_allocator);
            }
            if (self.cache_fetch_arrow_state) |fa| {
                fa.deinit(std.heap.smp_allocator);
            }
            try self.result_set.deinit();
        }

        pub fn columnNames(self: *@This()) ![][:0]const u8 {
            if (self.cache_column_names) |names| {
                return names.items;
            }

            const n_cols = self.result_set.n_cols;
            var names = try std.ArrayListUnmanaged([:0]const u8).initCapacity(
                std.heap.smp_allocator,
                n_cols,
            );
            errdefer names.deinit(std.heap.smp_allocator);
            errdefer for (names.items) |name| std.heap.smp_allocator.free(name);

            for (0..n_cols) |i_col| {
                const col_name = try self.result_set.stmt.colAttributeStringZ(
                    @intCast(i_col + 1),
                    .name,
                    std.heap.smp_allocator,
                );
                errdefer std.heap.smp_allocator.free(col_name);
                names.appendAssumeCapacity(col_name);
            }
            self.cache_column_names = names;
            return names.items;
        }

        pub fn tupleType(self: *@This()) !*c.PyTypeObject {
            if (self.cache_tuple_type) |tp| {
                return tp;
            }
            const names = try self.columnNames();
            const fields = try std.heap.smp_allocator.alloc(c.PyStructSequence_Field, names.len + 1);
            defer std.heap.smp_allocator.free(fields);
            fields[fields.len - 1] = c.PyStructSequence_Field{ .doc = null, .name = null };
            for (names, 0..) |name, i_name| {
                fields[i_name] = .{
                    .doc = null,
                    .name = name.ptr,
                };
            }

            var desc: c.PyStructSequence_Desc = .{
                .doc = "Rows from a zodbc query",
                .n_in_sequence = @intCast(fields.len - 1),
                .name = "zodbc.Row",
                .fields = fields.ptr,
            };
            const tp = c.PyStructSequence_NewType(&desc) orelse return PyErr;
            self.cache_tuple_type = tp;
            return tp;
        }

        pub fn fetchPyState(self: *@This()) !FetchPy {
            if (self.cache_fetch_py_state) |fp| {
                return fp;
            }
            const fp = try FetchPy.init(
                &self.result_set,
                std.heap.smp_allocator,
                self.stmt.dt7_fetch,
            );
            self.cache_fetch_py_state = fp;
            return fp;
        }

        pub fn fetchArrowState(self: *@This()) !FetchArrow {
            // TODO error when switching between fetch py/arrow?
            if (self.cache_fetch_arrow_state) |fa| {
                return fa;
            }
            const fa = try FetchArrow.init(
                &self.result_set,
                std.heap.smp_allocator,
                self.stmt.dt7_fetch,
            );
            self.cache_fetch_arrow_state = fa;
            return fa;
        }
    } = null,

    fn deinit(self: *Stmt) callconv(.c) void {
        deinit_err(self) catch {};
    }

    fn deinit_err(self: *Stmt) !void {
        if (self.result_set) |*result_set| {
            try result_set.deinit();
            self.result_set = null;
        }
        try self.stmt.deinit();
        c.Py_DECREF(self.env_con_caps);
    }
};
const StmtCapsule = py.PyCapsule(Stmt, "zodbc_stmt", Stmt.deinit);

pub fn connect(constr: []const u8) !Obj {
    const env = try zodbc.Environment.init(.v3_80);
    errdefer env.deinit() catch {};
    const con = try zodbc.Connection.init(env);
    errdefer con.deinit() catch {};
    try con.setConnectAttr(.{ .autocommit = .on });
    try con.connectWithString(constr);
    errdefer con.disconnect() catch {};

    const py_funcs = try PyFuncs.init();
    errdefer py_funcs.deinit();

    return try ConnectionCapsule.create_capsule(EnvCon{
        .env = env,
        .con = con,
        .py_funcs = py_funcs,
    });
}

pub fn setAutocommit(con: Obj, autocommit: bool) !void {
    const env_con = try ConnectionCapsule.read_capsule(con);
    try env_con.con.setConnectAttr(.{ .autocommit = if (autocommit) .on else .off });
}

pub fn getAutocommit(con: Obj) !bool {
    const env_con = try ConnectionCapsule.read_capsule(con);
    var odbc_buf: [1024]u8 = undefined;
    odbc_buf = std.mem.zeroes(@TypeOf(odbc_buf));
    const autocommit = try env_con.con.getConnectAttr(
        std.heap.smp_allocator,
        .autocommit,
        odbc_buf[0..],
    );
    return switch (autocommit.autocommit) {
        .on => true,
        .off => false,
    };
}

pub fn cursor(con: Obj, datetime2_7_fetch: utils.Dt7Fetch) !Obj {
    const env_con = try ConnectionCapsule.read_capsule(con);
    const stmt = try zodbc.Statement.init(env_con.con);
    errdefer stmt.deinit() catch {};
    return try StmtCapsule.create_capsule(.{
        .stmt = stmt,
        .env_con_caps = c.Py_NewRef(con),
        .env_con = env_con,
        .dt7_fetch = datetime2_7_fetch,
    });
}

pub fn execute(cur_obj: Obj, query: []const u8, py_params: Obj) !void {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set) |*result_set| {
        try result_set.deinit();
        // try cur.stmt.closeCursor();
        cur.result_set = null;
    }
    // Fixes issue with multiple execute calls without fetches but can error.
    // Maybe better to discard individual result sets?
    cur.stmt.closeCursor() catch {};

    var prepared = false;

    var params = try put_py.bindParams(
        cur.stmt,
        py_params,
        std.heap.smp_allocator,
        cur.env_con.py_funcs,
        &prepared,
        query,
    );
    defer put_py.deinitParams(&params, std.heap.smp_allocator);
    errdefer cur.stmt.free(.reset_params) catch {};

    var thread_state = c.PyEval_SaveThread();
    defer if (thread_state) |t_state| c.PyEval_RestoreThread(t_state);
    if (prepared) {
        cur.stmt.execute() catch |err| switch (err) {
            error.ExecuteNoData => {},
            else => return utils.odbcErrToPy(cur.stmt, "Execute", err, &thread_state),
        };
    } else {
        cur.stmt.execDirect(query) catch |err| switch (err) {
            error.ExecDirectNoData => {},
            else => return utils.odbcErrToPy(cur.stmt, "ExecDirect", err, &thread_state),
        };
    }

    cur.rowcount = cur.stmt.rowCount() catch |err|
        return utils.odbcErrToPy(cur.stmt, "RowCount", err, &thread_state);

    try cur.stmt.free(.reset_params);
}

pub fn fetchmany(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur, std.heap.smp_allocator);
    }
    return fetch_py(
        &try cur.result_set.?.fetchPyState(),
        &cur.result_set.?.result_set,
        std.heap.smp_allocator,
        n_rows,
        &cur.env_con.py_funcs,
        .tuple,
        void{},
        void{},
    );
}

pub fn fetchdicts(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur, std.heap.smp_allocator);
    }

    const names = try cur.result_set.?.columnNames();
    for (names[0 .. names.len - 1], 0..) |name, i_name| {
        for (names[i_name + 1 ..], 0..) |name2, i_name2| {
            if (std.mem.eql(u8, name, name2)) {
                return py.raise(
                    .ValueError,
                    "Column name '{s}' appears twice at positions {} and {}",
                    .{ name, i_name, i_name2 },
                );
            }
        }
    }

    return fetch_py(
        &try cur.result_set.?.fetchPyState(),
        &cur.result_set.?.result_set,
        std.heap.smp_allocator,
        n_rows,
        &cur.env_con.py_funcs,
        .dict,
        names,
        void{},
    );
}

pub fn fetchnamed(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur, std.heap.smp_allocator);
    }
    return fetch_py(
        &try cur.result_set.?.fetchPyState(),
        &cur.result_set.?.result_set,
        std.heap.smp_allocator,
        n_rows,
        &cur.env_con.py_funcs,
        .named,
        void{},
        try cur.result_set.?.tupleType(),
    );
}

pub fn exp_put(val: Obj, con: Obj) ![]const u8 {
    const env_con = try ConnectionCapsule.read_capsule(con);
    return @tagName(try @import("put_py.zig").Conv.fromValue(val, env_con.py_funcs));
}

pub fn getinfo(con: Obj, info_name: []const u8) !Obj {
    const env_con = try ConnectionCapsule.read_capsule(con);
    // inline for (@typeInfo(zodbc.odbc.info.InfoTypeString).@"enum") |field| {
    const info_e = inline for (comptime std.enums.values(zodbc.odbc.info.InfoTypeString)) |info_e| {
        if (std.mem.eql(u8, @tagName(info_e), info_name)) {
            break info_e;
        }
    } else return py.raise(.NotImplemented, "getinfo for {s} not implemented", .{info_name});
    const info = try env_con.con.getInfoString(
        std.heap.smp_allocator,
        info_e,
    );
    defer std.heap.smp_allocator.free(info);
    return c.PyUnicode_FromStringAndSize(info.ptr, @intCast(info.len)) orelse return PyErr;
}

pub fn commit(con: Obj) !void {
    const env_con = try ConnectionCapsule.read_capsule(con);
    const thread_state = c.PyEval_SaveThread();
    defer c.PyEval_RestoreThread(thread_state);
    try env_con.con.endTran(.commit);
}

pub fn rollback(con: Obj) !void {
    const env_con = try ConnectionCapsule.read_capsule(con);
    const thread_state = c.PyEval_SaveThread();
    defer c.PyEval_RestoreThread(thread_state);
    try env_con.con.endTran(.rollback);
}

pub fn nextset(cur_obj: Obj) !bool {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set) |*result_set| {
        try result_set.deinit();
        cur.result_set = null;
    }
    var thread_state = c.PyEval_SaveThread();
    defer if (thread_state) |t_state| c.PyEval_RestoreThread(t_state);
    cur.stmt.moreResults() catch |err| switch (err) {
        error.MoreResultsNoData => return false,
        else => return utils.odbcErrToPy(cur.stmt, "MoreResults", err, &thread_state),
    };
    cur.rowcount = cur.stmt.rowCount() catch |err|
        return utils.odbcErrToPy(cur.stmt, "RowCount", err, &thread_state);

    return true;
}

pub fn con_close(con: Obj) !void {
    const env_con = try ConnectionCapsule.read_capsule(con);
    try env_con.close();
}

pub fn con_closed(con: Obj) !bool {
    const env_con = try ConnectionCapsule.read_capsule(con);
    return env_con.closed;
}

pub fn cur_deinit(cur: Obj) !void {
    const stmt = try StmtCapsule.read_capsule(cur);
    try stmt.deinit_err();
}

pub fn rowcount(cur_obj: Obj) !i64 {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    return cur.rowcount;
}

pub fn cancel(cur_obj: Obj) !void {
    var thread_state = c.PyEval_SaveThread();
    defer if (thread_state) |t_state| c.PyEval_RestoreThread(t_state);
    const cur = try StmtCapsule.read_capsule(cur_obj);
    cur.stmt.cancel() catch |err| switch (err) {
        error.CancelSuccessWithInfo => {}, // happens sometimes with sql server and no info is provided
        else => return utils.odbcErrToPy(cur.stmt, "Cancel", err, &thread_state),
    };
}

const SchemaCapsule = py.PyCapsule(arrow.ArrowSchema, "arrow_schema", &struct {
    fn deinit(self: *arrow.ArrowSchema) callconv(.c) void {
        _ = self;
        // if (self.release) |release|
        //     release(self);
    }
}.deinit);
const ArrayCapsule = py.PyCapsule(arrow.ArrowArray, "arrow_array", &struct {
    fn deinit(self: *arrow.ArrowArray) callconv(.c) void {
        _ = self;
        // if (self.release) |release|
        //     release(self);
    }
}.deinit);

pub fn arrow_batch(cur_obj: Obj, n_rows: usize) !struct { Obj, Obj } {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur, arrow.ally);
    }

    const fetch_arrow = try cur.result_set.?.fetchArrowState();

    const schema, const array = try fetch_arrow.fetch_batch(
        &cur.result_set.?.result_set,
        arrow.ally,
        n_rows,
    );

    return .{
        try SchemaCapsule.create_capsule(schema),
        try ArrayCapsule.create_capsule(array),
    };
}
