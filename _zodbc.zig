const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;

const PyErr = py.PyErr;

const EnvCon = struct {
    env: zodbc.Environment,
    con: zodbc.Connection,
};
const ConnectionCapsule = py.PyCapsule(EnvCon, "arrow_array", &struct {
    fn deinit(self: *EnvCon) callconv(.c) void {
        self.con.disconnect() catch unreachable;
        self.con.deinit();
        self.env.deinit();
    }
}.deinit);

const Stmt = struct {
    stmt: zodbc.Statement,
    /// Keep a reference to the connection capsule so it isn't
    /// accidentally garbage collected before all its statements
    env_con_caps: Obj,
    result_set: ?zodbc.ResultSet = null,
};
const StmtCapsule = py.PyCapsule(Stmt, "arrow_array", &struct {
    fn deinit(self: *Stmt) callconv(.c) void {
        if (self.result_set) |*result_set| {
            result_set.deinit();
        }
        self.stmt.deinit();
        c.Py_DECREF(self.env_con_caps);
    }
}.deinit);

pub fn connect(constr: []const u8) !Obj {
    const env = try zodbc.Environment.init(.v3_80);
    errdefer env.deinit();
    const con = try zodbc.Connection.init(env);
    errdefer con.deinit();
    try con.setConnectAttr(.{ .autocommit = .on });
    try con.connectWithString(constr);
    errdefer con.disconnect() catch unreachable;

    return try ConnectionCapsule.create_capsule(EnvCon{
        .env = env,
        .con = con,
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

pub fn cursor(con: Obj) !Obj {
    const env_con = try ConnectionCapsule.read_capsule(con);
    const stmt = try zodbc.Statement.init(env_con.con);
    errdefer stmt.deinit();
    return try StmtCapsule.create_capsule(.{
        .stmt = stmt,
        .env_con_caps = c.Py_NewRef(con),
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

pub fn fetch_many(cur_obj: Obj, n_rows: usize) !Obj {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur.stmt, std.heap.smp_allocator);
    }
    return @import("fetch_py.zig").fetch_py(
        &cur.result_set.?,
        std.heap.smp_allocator,
        n_rows,
    );
}
