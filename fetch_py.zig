const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;

const CDataType = zodbc.odbc.types.CDataType;

pub fn fetch_py(
    res: *zodbc.ResultSet,
    allocator: std.mem.Allocator,
    n_rows: usize,
) !Obj {
    const cycle = try allocator.alloc(CDataType, res.n_cols + 1);
    errdefer allocator.free(cycle);
    for (res.columns.items, 0..) |col, it| {
        cycle[it] = col.c_type;
    }
    cycle[cycle.len - 1] = .default; // dummy end
    var rows = try std.ArrayListUnmanaged(Obj).initCapacity(allocator, n_rows);
    errdefer rows.deinit(allocator);
    errdefer for (rows.items) |row| c.Py_XDECREF(row);
    var i_col: usize = 0;
    var i_row: usize = 0;
    sw: switch (CDataType.ard_type) {
        // dummy start
        .ard_type => {
            if (try res.borrowRow() == null) {
                break :sw;
            }
            rows.appendAssumeCapacity(c.PyTuple_New(@intCast(res.n_cols)) orelse return py.PyErr);
            continue :sw cycle[i_col];
        },
        // dummy end
        .default => {
            i_row += 1;
            if (i_row >= n_rows) {
                break :sw;
            }
            i_col = 0;
            continue :sw .ard_type;
        },
        inline else => |c_type| {
            const T = if (c_type.MaybeType()) |t| t else @panic("c_type not implemented: " ++ @tagName(c_type));
            const cell = res.borrowed_row[i_col] orelse {
                c.Py_INCREF(c.Py_None());
                _ = c.PyTuple_SetItem(rows.items[i_row], @intCast(i_col), c.Py_None());
                i_col += 1;
                continue :sw cycle[i_col];
            };
            const py_val = switch (@typeInfo(T)) {
                .int, .float => try py.zig_to_py(c_type.asTypeValue(cell)),
                else => try py.zig_to_py(c_type.asTypeValue(cell)),
            };
            _ = c.PyTuple_SetItem(rows.items[i_row], @intCast(i_col), py_val);
            i_col += 1;
            continue :sw cycle[i_col];
        },
    }

    const py_ret = c.PyList_New(@intCast(rows.items.len)) orelse return py.PyErr;
    errdefer c.Py_DECREF(py_ret);
    for (rows.items, 0..) |row, ix| {
        if (c.PyList_SetItem(py_ret, @intCast(ix), row) == -1)
            return py.PyErr;
    }
    return py_ret;
}
