const py = @import("py");
const c = @import("c");
const Obj = *c.PyObject;

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
