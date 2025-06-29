const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const PyFuncs = @import("PyFuncs.zig");
const FetchPy = @import("fetch_py.zig");
const fetch_py = FetchPy.fetch_py;
const c = py.py;
const Obj = *c.PyObject;

const PyErr = py.PyErr;

const EnvCon = struct {
    env: zodbc.Environment,
    con: zodbc.Connection,
    py_funcs: PyFuncs,

    fn deinit(self: *EnvCon) callconv(.c) void {
        self.con.disconnect() catch unreachable;
        self.con.deinit();
        self.env.deinit();
        self.py_funcs.deinit();
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
    dt7_fetch: FetchPy.Dt7Fetch,
    result_set: ?struct {
        result_set: zodbc.ResultSet,
        dt7_fetch: FetchPy.Dt7Fetch,
        cache_column_names: ?std.ArrayListUnmanaged([:0]const u8) = null,
        cache_tuple_type: ?*c.PyTypeObject = null,
        cache_fetch_py_state: ?FetchPy = null,

        fn init(stmt: Stmt, allocator: std.mem.Allocator) !@This() {
            return .{
                .result_set = try .init(
                    stmt.stmt,
                    allocator,
                ),
                .dt7_fetch = stmt.dt7_fetch,
            };
        }

        fn deinit(self: *@This()) void {
            self.result_set.deinit();
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
                self.dt7_fetch,
            );
            self.cache_fetch_py_state = fp;
            return fp;
        }
    } = null,

    fn deinit(self: *Stmt) callconv(.c) void {
        if (self.result_set) |*result_set| {
            result_set.deinit();
        }
        self.stmt.deinit();
        c.Py_DECREF(self.env_con_caps);
    }
};
const StmtCapsule = py.PyCapsule(Stmt, "zodbc_stmt", Stmt.deinit);

pub fn connect(constr: []const u8) !Obj {
    const env = try zodbc.Environment.init(.v3_80);
    errdefer env.deinit();
    const con = try zodbc.Connection.init(env);
    errdefer con.deinit();
    try con.setConnectAttr(.{ .autocommit = .on });
    try con.connectWithString(constr);
    errdefer con.disconnect() catch unreachable;

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

pub fn cursor(con: Obj, datetime2_7_fetch: FetchPy.Dt7Fetch) !Obj {
    const env_con = try ConnectionCapsule.read_capsule(con);
    const stmt = try zodbc.Statement.init(env_con.con);
    errdefer stmt.deinit();
    return try StmtCapsule.create_capsule(.{
        .stmt = stmt,
        .env_con_caps = c.Py_NewRef(con),
        .env_con = env_con,
        .dt7_fetch = datetime2_7_fetch,
    });
}

pub fn execute(cur_obj: Obj, query: []const u8) !void {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set) |*result_set| {
        cur.result_set = null;
        result_set.deinit();
    }
    try cur.stmt.execDirect(query);
}

pub fn fetch_many(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur.*, std.heap.smp_allocator);
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

pub fn fetch_dicts(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur.*, std.heap.smp_allocator);
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

pub fn fetch_named(cur_obj: Obj, n_rows: ?usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur.*, std.heap.smp_allocator);
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
