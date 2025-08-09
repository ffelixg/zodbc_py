const std = @import("std");
const py = @import("py");
const c = @import("c");
const Obj = *c.PyObject;
const zodbc = @import("zodbc");
const PyErr = py.PyErr;

pub inline fn pyCall(func: Obj, args: anytype) !Obj {
    // without limited api, PyObject_Vectorcall would give better performance
    const py_args = try @call(
        .always_inline,
        py.zig_to_py,
        .{args},
    );
    defer c.Py_DECREF(py_args);
    return c.PyObject_Call(
        func,
        py_args,
        null,
    ) orelse return py.PyErr;
}

pub inline fn attrsToStruct(comptime T: type, obj: Obj) !T {
    var result: T = undefined;

    inline for (@typeInfo(T).@"struct".fields) |field| {
        const py_value = c.PyObject_GetAttrString(obj, field.name) orelse return py.PyErr;
        defer c.Py_DECREF(py_value);

        const value = try py.py_to_zig(field.type, py_value, null);
        @field(result, field.name) = value;
    }

    return result;
}

pub fn odbcErrToPy(has_handle: anytype, comptime name: []const u8, err: anytype, thread_state_ptr: ?*?*c.PyThreadState) error{PyErr} {
    if (thread_state_ptr) |thread_state| {
        if (thread_state.*) |ts| {
            c.PyEval_RestoreThread(ts);
            thread_state.* = null;
        }
    }

    switch (err) {
        inline else => |e| {
            const err_name = @errorName(e);
            if (comptime std.mem.eql(u8, err_name, "OutOfMemory")) {
                _ = c.PyErr_NoMemory();
                return PyErr;
            }
            if (comptime std.mem.eql(u8, err_name, "InvalidWtf8")) {
                return py.raise(.ValueError, "Encoding error while calling {s}\n", .{name});
            }
            if (comptime std.mem.eql(u8, err_name[0..name.len], name)) {
                const suffix = err_name[name.len..];
                if (comptime (std.mem.eql(u8, suffix, "Error") or std.mem.eql(u8, suffix, "SuccessWithInfo"))) {
                    const recs = zodbc.getDiagRecs(
                        has_handle,
                        std.heap.smp_allocator,
                    ) catch {
                        return py.raise(.Exception, "Failed to get odbc diagnostics for {s}", .{name});
                    };
                    defer recs.deinit(std.heap.smp_allocator);
                    if (recs.items.len == 0) {
                        return py.raise(.Exception, "Driver indicated {s} for {s} but did not provide diagnostic information", .{ suffix, name });
                    }
                    const rec = recs.items[0];
                    return py.raise(.Exception, "{s}: [{s}]{s} (native: {})", .{ err_name, rec.sql_state, rec.message, rec.native_error });
                } else if (comptime std.mem.eql(u8, suffix, "InvalidHandle")) {
                    return py.raise(.Exception, "Tried to call {s} with an invalid handle. Maybe the connection was closed?", .{name});
                }
                return py.raise(.Exception, "Encountered {s} while calling {s}\n", .{ suffix, name });
            }
            @compileError("Need to implement error handling for " ++ err_name);
        },
    }
}

pub const Dt7Fetch = enum(u4) { micro = 1, string = 2, nano = 3 };

pub fn ensurePrepared(
    stmt: zodbc.Statement,
    prepared: *bool,
    query: []const u8,
    thread_state: ?*?*c.PyThreadState,
) !void {
    if (prepared.*) return;
    stmt.prepare(query) catch |err| return odbcErrToPy(stmt, "Prepare", err, thread_state);
    prepared.* = true;
}

pub fn raise(
    exc: anytype,
    comptime msg: []const u8,
    args: anytype,
    thread_state_ptr: ?*?*c.PyThreadState,
) error{PyErr} {
    if (thread_state_ptr) |thread_state| {
        if (thread_state.*) |ts| {
            c.PyEval_RestoreThread(ts);
            thread_state.* = null;
        }
    }

    return py.raise(exc, msg, args);
}
