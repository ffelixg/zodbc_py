const std = @import("std");
const py = @import("py");
const zodbc = @import("zodbc");
const c = py.py;
const Obj = *c.PyObject;
const PyFuncs = @import("PyFuncs.zig");
const fmt = @import("fmt.zig");
const utils = @import("utils.zig");
const pyCall = utils.pyCall;
const Dt7Fetch = utils.Dt7Fetch;
const arrow = @import("arrow.zig");

const CDataType = zodbc.odbc.types.CDataType;

const Conversions = union(enum) {
    begin_row: void,
    end_row: void,

    wchar: CDataType.wchar.Type(),
    sshort: CDataType.sshort.Type(),
    ushort: CDataType.ushort.Type(),
    slong: CDataType.slong.Type(),
    ulong: CDataType.ulong.Type(),
    float: CDataType.float.Type(),
    double: CDataType.double.Type(),
    bit: CDataType.bit.Type(),
    stinyint: CDataType.stinyint.Type(),
    utinyint: CDataType.utinyint.Type(),
    sbigint: CDataType.sbigint.Type(),
    ubigint: CDataType.ubigint.Type(),
    binary: CDataType.binary.Type(),
    numeric: CDataType.numeric.Type(), // TODO maybe implement different bit widths?
    guid: CDataType.guid.Type(),
    type_date: CDataType.type_date.Type(),
    type_time: CDataType.type_time.Type(),
    type_timestamp_second: CDataType.type_timestamp.Type(),
    type_timestamp_milli: CDataType.type_timestamp.Type(),
    type_timestamp_micro: CDataType.type_timestamp.Type(),
    type_timestamp_nano: CDataType.type_timestamp.Type(),
    type_timestamp_string: CDataType.type_timestamp.Type(),
    ss_timestampoffset_second: CDataType.ss_timestampoffset.Type(),
    ss_timestampoffset_milli: CDataType.ss_timestampoffset.Type(),
    ss_timestampoffset_micro: CDataType.ss_timestampoffset.Type(),
    ss_timestampoffset_nano: CDataType.ss_timestampoffset.Type(),
    ss_timestampoffset_string: CDataType.ss_timestampoffset.Type(),
    ss_time2_second: CDataType.ss_time2.Type(),
    ss_time2_milli: CDataType.ss_time2.Type(),
    ss_time2_micro: CDataType.ss_time2.Type(),
    ss_time2_nano: CDataType.ss_time2.Type(),

    const Tags = @typeInfo(@This()).@"union".tag_type.?;

    fn Type(tag: Tags) type {
        return @FieldType(@This(), @tagName(tag));
    }

    fn ArrowType(tag: Tags) type {
        return switch (tag) {
            .wchar => u32,
            .sshort => Conversions.Type(tag),
            .ushort => Conversions.Type(tag),
            .slong => Conversions.Type(tag),
            .ulong => Conversions.Type(tag),
            .float => Conversions.Type(tag),
            .double => Conversions.Type(tag),
            .bit => std.DynamicBitSetUnmanaged.MaskInt,
            .stinyint => Conversions.Type(tag),
            .utinyint => Conversions.Type(tag),
            .sbigint => Conversions.Type(tag),
            .ubigint => Conversions.Type(tag),
            .binary => u32,
            .numeric => i128,
            .guid => Conversions.Type(tag),
            .type_date => i32,
            .type_time => i32,
            .type_timestamp_second => i64,
            .type_timestamp_milli => i64,
            .type_timestamp_micro => i64,
            .type_timestamp_nano => i64,
            .type_timestamp_string => u32,
            .ss_timestampoffset_second => i64,
            .ss_timestampoffset_milli => i64,
            .ss_timestampoffset_nano => i64,
            .ss_timestampoffset_micro => i64,
            .ss_timestampoffset_string => u32,
            .ss_time2_second => i32,
            .ss_time2_milli => i32,
            .ss_time2_micro => i64,
            .ss_time2_nano => i64,
            .begin_row, .end_row => unreachable,
        };
    }

    fn isVarArrow(tag: Tags) bool {
        return switch (tag) {
            .wchar, .binary, .ss_timestampoffset_string, .type_timestamp_string => true,
            else => false,
        };
    }

    fn asTypeValue(comptime tag: Tags, data: []u8) Type(tag) {
        return std.mem.bytesToValue(Type(tag), data);
    }
};

comptime {
    @setEvalBranchQuota(0xFFFF_FFFF);
    for (std.enums.values(Conversions.Tags)) |tag| {
        var found_match = false;
        for (std.enums.values(zodbc.odbc.types.CDataType)) |c_type| {
            const tagn = @tagName(tag);
            const ctn = @tagName(c_type);
            if (std.mem.eql(u8, tagn, ctn)) {
                found_match = true;
                std.debug.assert(c_type.Type() == Conversions.Type(tag));
            }
            if (tagn.len >= ctn.len and tagn[ctn.len] == '_' and std.mem.eql(u8, tagn[0..ctn.len], ctn)) {
                found_match = true;
                std.debug.assert(c_type.Type() == Conversions.Type(tag));
            }
        }
        if (!found_match and tag != .begin_row and tag != .end_row) {
            @compileError("Conversion " ++ @tagName(tag) ++ " does not match any CDataType");
        }
    }
}

const Schema = struct {
    name: []const u8,
    format: []const u8,

    /// Clones contents of Schema
    fn produce(self: @This()) !arrow.ArrowSchema {
        const Private = struct {
            name: [:0]u8,
            format: [:0]u8,

            fn deinit(private: *@This()) void {
                arrow.ally.free(private.name);
                arrow.ally.free(private.format);
                arrow.ally.destroy(private);
            }
        };

        const name = try arrow.ally.dupeZ(u8, self.name);
        errdefer arrow.ally.free(name);
        const format = try arrow.ally.dupeZ(u8, self.format);
        errdefer arrow.ally.free(format);
        const private = try arrow.ally.create(Private);
        private.* = Private{
            .name = name,
            .format = format,
        };
        errdefer arrow.ally.destroy(private);

        return arrow.ArrowSchema{
            .name = name.ptr,
            .format = format.ptr,
            .release = struct {
                fn release(schema: *arrow.ArrowSchema) callconv(.c) void {
                    const private_inner: *Private = @alignCast(@ptrCast(schema.private_data));
                    private_inner.deinit();
                    schema.release = null;
                }
            }.release,
            .private_data = @ptrCast(private),
        };
    }

    fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.format);
    }
};

fn bitSetLen(n_rows: usize) usize {
    return std.math.divCeil(usize, n_rows, @bitSizeOf(std.DynamicBitSetUnmanaged.MaskInt)) catch unreachable;
}

const Array = struct {
    data: ?[]u8,
    data_current: usize = 0,
    value: []u8,
    valid_mem: []std.DynamicBitSetUnmanaged.MaskInt,
    tag: Conversions.Tags,
    ownership_stolen: bool = false,
    n_rows_max: usize,

    inline fn valid(self: *@This()) std.DynamicBitSetUnmanaged {
        return .{ .bit_length = self.n_rows_max, .masks = self.valid_mem.ptr };
    }

    fn init(n_rows: usize, tag: Conversions.Tags, allocator: std.mem.Allocator) !@This() {
        const valid_mem = try allocator.alloc(std.DynamicBitSetUnmanaged.MaskInt, bitSetLen(n_rows));
        errdefer allocator.free(valid_mem);
        @memset(valid_mem, 0);

        switch (tag) {
            .begin_row, .end_row => unreachable,
            inline else => |comp_tag| {
                const T = Conversions.ArrowType(comp_tag);
                if (comptime Conversions.isVarArrow(comp_tag)) {
                    comptime std.debug.assert(T == u32);
                    const value = try allocator.alloc(T, n_rows + 1);
                    errdefer allocator.free(value);
                    value[0] = 0;
                    const data = try allocator.alloc(u8, n_rows * 42);
                    errdefer allocator.free(data);
                    return .{
                        .data = data,
                        .value = @ptrCast(value),
                        .valid_mem = valid_mem,
                        .tag = comp_tag,
                        .n_rows_max = n_rows,
                    };
                }
                if (comp_tag == .bit) {
                    comptime std.debug.assert(T == std.DynamicBitSetUnmanaged.MaskInt);
                    const bitset_mem = try allocator.alloc(T, bitSetLen(n_rows));
                    errdefer allocator.free(bitset_mem);
                    @memset(bitset_mem, 0);
                    return .{
                        .data = null,
                        .value = @ptrCast(bitset_mem),
                        .valid_mem = valid_mem,
                        .tag = comp_tag,
                        .n_rows_max = n_rows,
                    };
                }
                const value = try allocator.alloc(T, n_rows);
                errdefer allocator.free(value);
                return .{
                    .data = null,
                    .value = @ptrCast(value),
                    .valid_mem = valid_mem,
                    .tag = comp_tag,
                    .n_rows_max = n_rows,
                };
            },
        }
    }

    fn deinit(self: *const @This(), allocator: std.mem.Allocator) void {
        if (self.ownership_stolen) return;
        if (self.data) |data| {
            allocator.free(data);
        }
        allocator.free(self.valid_mem);

        switch (self.tag) {
            .begin_row, .end_row => unreachable,
            inline else => |comp_tag| {
                allocator.free(@as([]Conversions.ArrowType(comp_tag), @ptrCast(@alignCast(self.value))));
            },
        }
    }

    /// Steals the ownership of self
    fn produce(self: *@This(), n_rows: usize) !arrow.ArrowArray {
        const buffers = try arrow.ally.alloc(?[*]u8, 3);
        errdefer arrow.ally.free(buffers);

        buffers[0] = @ptrCast(self.valid_mem.ptr);
        buffers[1] = self.value.ptr;
        if (self.data) |data| {
            buffers[2] = data.ptr;
            std.debug.print("data ptr: {*}\n", .{data.ptr});
        } else {
            buffers[2] = null;
        }
        self.ownership_stolen = true;
        return .{
            .length = @intCast(n_rows),
            .null_count = @intCast(n_rows - self.valid().count()),
            .n_buffers = if (self.data != null) 3 else 2,
            .buffers = buffers.ptr,
            .private_data = @ptrCast(self),
        };
    }
};

cycle: []Conversions.Tags,
cols: []Schema,

pub fn init(res: *const zodbc.ResultSet, allocator: std.mem.Allocator, dt7_fetch: Dt7Fetch) !@This() {
    var cols: std.ArrayListUnmanaged(Schema) = try .initCapacity(allocator, res.n_cols);
    defer cols.deinit(allocator);
    defer for (cols.items) |col| col.deinit(allocator);

    const cycle = try allocator.alloc(Conversions.Tags, res.n_cols + 1);
    errdefer allocator.free(cycle);
    for (res.columns.items, 0..) |col, i_col| {
        cycle[i_col] = switch (col.c_type) {
            inline else => |c_type| blk_outer: {
                const tag: Conversions.Tags, const fmt_raw = switch (c_type) {
                    .wchar => .{ .wchar, "u" },
                    .sshort => .{ .sshort, "s" },
                    .ushort => .{ .ushort, "S" },
                    .slong => .{ .slong, "i" },
                    .ulong => .{ .ulong, "I" },
                    .float => .{ .float, "f" },
                    .double => .{ .double, "g" },
                    .bit => .{ .bit, "b" },
                    .stinyint => .{ .stinyint, "c" },
                    .utinyint => .{ .utinyint, "C" },
                    .sbigint => .{ .sbigint, "l" },
                    .ubigint => .{ .ubigint, "L" },
                    .binary => .{ .binary, "z" },
                    .guid => .{ .guid, "w:16" },
                    .numeric => .{ .numeric, "d:{},{}" },
                    .type_date => .{ .type_date, "tdD" },
                    .type_time => .{ .type_time, "tts" },
                    .type_timestamp => blk: {
                        const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                        std.debug.assert(prec >= 0);
                        if (prec == 0) {
                            break :blk .{ .type_timestamp_second, "tss" };
                        } else if (prec <= 3) {
                            break :blk .{ .type_timestamp_milli, "tsm" };
                        } else if (prec <= 6) {
                            break :blk .{ .type_timestamp_micro, "tsu" };
                        } else if (prec <= 9) {
                            switch (dt7_fetch) {
                                .micro => break :blk .{ .type_timestamp_micro, "tsu" },
                                .nano => break :blk .{ .type_timestamp_nano, "tsn" },
                                .string => break :blk .{ .type_timestamp_string, "u" },
                            }
                        } else {
                            unreachable;
                        }
                    },
                    .ss_timestampoffset => blk: {
                        const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                        std.debug.assert(prec >= 0);
                        if (prec == 0) {
                            break :blk .{ .ss_timestampoffset_second, "tss:+00:00" };
                        } else if (prec <= 3) {
                            break :blk .{ .ss_timestampoffset_milli, "tsm:+00:00" };
                        } else if (prec <= 6) {
                            break :blk .{ .ss_timestampoffset_micro, "tsu:+00:00" };
                        } else if (prec <= 9) {
                            switch (dt7_fetch) {
                                .micro => break :blk .{ .ss_timestampoffset_micro, "tsu:+00:00" },
                                .nano => break :blk .{ .ss_timestampoffset_nano, "tsn:+00:00" },
                                .string => break :blk .{ .ss_timestampoffset_string, "u" },
                            }
                        } else {
                            unreachable;
                        }
                    },
                    .ss_time2 => blk: {
                        const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                        std.debug.assert(prec >= 0);
                        if (prec == 0) {
                            break :blk .{ .ss_time2_second, "tts" };
                        } else if (prec <= 3) {
                            break :blk .{ .ss_time2_milli, "ttm" };
                        } else if (prec <= 6) {
                            break :blk .{ .ss_time2_micro, "ttu" };
                        } else if (prec <= 9) {
                            break :blk .{ .ss_time2_nano, "ttn" };
                        } else {
                            unreachable;
                        }
                    },
                    // .type_timestamp, .ss_timestampoffset, .ss_time2 => blk: {
                    //     const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                    //     std.debug.assert(prec >= 0);
                    //     const prefix, const suffix = switch (c_type) {
                    //         .ss_timestampoffset => .{ "ts", "+00:00" },
                    //         .type_timestamp => .{ "ts", "" },
                    //         .ss_time2 => .{ "tt", "" },
                    //         else => unreachable,
                    //     };
                    //     switch (prec) {
                    //         inline 0 => break :blk .{ std.meta.stringToEnum(Conversions.Tags, @tagName(c_type) ++ "_second"), prefix ++ "s" ++ suffix },
                    //         inline 7...9 => blk: {
                    //             if (c_type == .ss_time2) {
                    //                 break :blk .{ std.meta.stringToEnum(Conversions.Tags, @tagName(c_type) ++ "_nano"), prefix ++ "n" ++ suffix };
                    //             }
                    //             switch (dt7_fetch) {
                    //                 .micro => break :blk .{ std.meta.stringToEnum(Conversions.Tags, @tagName(c_type) ++ "_micro"), prefix ++ "u" ++ suffix },
                    //                 .nano => break :blk .{ std.meta.stringToEnum(Conversions.Tags, @tagName(c_type) ++ "_nano"), prefix ++ "n" ++ suffix },
                    //                 .string => break :blk .{ std.meta.stringToEnum(Conversions.Tags, @tagName(c_type) ++ "_string"), "u" },
                    //             }
                    //         },
                    //     }
                    else => return error.ConversionNotImplemented,
                };
                const format = switch (col.c_type) {
                    // TODO make fmt_raw comptime?
                    .numeric => try std.fmt.allocPrint(allocator, "d:{},{}", .{
                        try res.stmt.colAttribute(@intCast(i_col + 1), .precision),
                        try res.stmt.colAttribute(@intCast(i_col + 1), .scale),
                    }),
                    else => try allocator.dupe(u8, fmt_raw),
                };
                errdefer allocator.free(format);

                const name = try res.stmt.colAttributeString(@intCast(i_col + 1), .name, allocator);
                errdefer allocator.free(name);

                cols.appendAssumeCapacity(.{ .name = name, .format = format });
                break :blk_outer tag;
            },
        };
    }

    cycle[cycle.len - 1] = .end_row;
    return .{ .cycle = cycle, .cols = try cols.toOwnedSlice(allocator) };
}

pub fn deinit(self: *const @This(), allocator: std.mem.Allocator) void {
    for (self.cols) |col| {
        col.deinit(allocator);
    }
    allocator.free(self.cols);
    allocator.free(self.cycle);
}

pub fn fetch_batch(
    self: *const @This(),
    res: *zodbc.ResultSet,
    allocator: std.mem.Allocator,
    n_rows: usize,
) !struct { arrow.ArrowSchema, arrow.ArrowArray } {
    var arrays: std.ArrayListUnmanaged(Array) = try .initCapacity(allocator, res.n_cols);
    errdefer arrays.deinit(allocator);
    errdefer for (arrays.items) |array| array.deinit(arrow.ally);

    for (self.cycle[0..res.n_cols]) |conv| {
        arrays.appendAssumeCapacity(try .init(n_rows, conv, arrow.ally));
    }

    var thread_state = c.PyEval_SaveThread();
    defer if (thread_state) |ts| c.PyEval_RestoreThread(ts);
    _ = &thread_state;
    var i_col: usize = 0;
    var i_row: usize = 0;
    sw: switch (Conversions.Tags.begin_row) {
        .begin_row => {
            if (try res.borrowRow() == null)
                break :sw;
            std.debug.assert(i_col == 0);
            continue :sw self.cycle[0];
        },
        .end_row => {
            i_row += 1;
            if (i_row >= n_rows) {
                break :sw;
            }
            i_col = 0;
            continue :sw .begin_row;
        },
        inline else => |conv| {
            const arr = &arrays.items[i_col];
            const values: []Conversions.ArrowType(conv) = @ptrCast(@alignCast(arr.value));
            if (res.borrowed_row[i_col]) |bytes| {
                var valid = arr.valid();
                valid.set(i_row);
                if (comptime Conversions.isVarArrow(conv)) {
                    try odbcToArrowVar(
                        bytes,
                        values,
                        i_row,
                        conv,
                        arr,
                    );
                } else {
                    values[i_row] = try odbcToArrowScalar(bytes, conv);
                }
            } else {
                var valid = arr.valid();
                valid.unset(i_row);
                if (Conversions.isVarArrow(conv)) {
                    values[i_row + 1] = values[i_row];
                }
            }
            i_col += 1;
            continue :sw self.cycle[i_col];
        },
    }

    var batch_schema = try produceBatchSchema(self.cols);
    errdefer batch_schema.release.?(&batch_schema);

    const batch_array = try produceBatchArray(arrays.items, i_row);
    errdefer batch_array.release.?(batch_array);

    std.debug.print("{any}\n", .{@as([]u32, @ptrCast(@alignCast(batch_array.children.?[0].buffers[1].?[0 .. 4 * 3])))});

    return .{ batch_schema, batch_array };
}

inline fn odbcToArrowVar(
    bytes: []u8,
    values: []u32,
    i_row: usize,
    comptime conv: Conversions.Tags,
    arr: *Array,
) !void {
    var data = arr.data.?;
    const min_available = (std.math.divCeil(usize, bytes.len * 2, 2) catch unreachable) * 3;
    if (data.len - values[i_row] < min_available) {
        data = arrow.ally.realloc(
            data,
            @max(@divFloor(data.len, 2) * 3, data.len + min_available),
        ) catch unreachable;
        arr.data = data;
    }

    const bytes_T: []Conversions.Type(conv) = @ptrCast(@alignCast(bytes));
    switch (conv) {
        .binary => {
            @memcpy(data[values[i_row] .. values[i_row] + bytes_T.len], bytes_T);
            values[i_row + 1] = values[i_row] + @as(u32, @intCast(bytes_T.len));
        },
        .wchar => {
            const len = std.unicode.wtf16LeToWtf8(data[values[i_row]..], bytes_T);
            values[i_row + 1] = values[i_row] + @as(u32, @intCast(len));
        },
        .ss_timestampoffset_string, .type_timestamp_string => {
            @panic("TODO");
        },
        else => @compileError(@tagName(conv) ++ " is not a variable length type"),
    }
}

inline fn odbcToArrowScalar(
    bytes: []u8,
    comptime conv: Conversions.Tags,
) !Conversions.ArrowType(conv) {
    const val = Conversions.asTypeValue(conv, bytes);

    _ = val;
    std.debug.print("conv: {s}\n", .{@tagName(conv)});
    return error.TODO;
}

/// Clones the Schemas
fn produceBatchSchema(schemas: []Schema) !arrow.ArrowSchema {
    var schema_children: std.ArrayListUnmanaged(*arrow.ArrowSchema) = try .initCapacity(arrow.ally, schemas.len);
    defer schema_children.deinit(arrow.ally);
    errdefer for (schema_children.items) |child| {
        child.release.?(child);
        arrow.ally.destroy(child);
    };

    for (schemas) |schema| {
        var child = try schema.produce();
        errdefer child.release.?(&child);

        const child_ptr = try arrow.ally.create(arrow.ArrowSchema);
        errdefer arrow.ally.destroy(child_ptr);
        child_ptr.* = child;
        schema_children.appendAssumeCapacity(child_ptr);
    }

    const schema_children_slice = try schema_children.toOwnedSlice(arrow.ally);
    errdefer arrow.ally.free(schema_children_slice);

    return .{
        .format = "+s",
        .name = "",
        .n_children = @intCast(schemas.len),
        .children = schema_children_slice.ptr,
        .release = struct {
            fn release_batch_schema(self: *arrow.ArrowSchema) callconv(.c) void {
                std.debug.assert(self.release != null);
                std.debug.assert(self.children != null);
                for (self.children.?[0..@intCast(self.n_children)]) |child| {
                    child.release.?(child);
                    arrow.ally.destroy(child);
                }
                arrow.ally.free(self.children.?[0..@intCast(self.n_children)]);
                self.release = null;
            }
        }.release_batch_schema,
    };
}

/// Steals ownership (TODO only if successful)
fn produceBatchArray(arrays: []Array, n_rows: usize) !arrow.ArrowArray {
    var array_children: std.ArrayListUnmanaged(*arrow.ArrowArray) = try .initCapacity(arrow.ally, arrays.len);
    defer array_children.deinit(arrow.ally);
    errdefer for (array_children.items) |child| {
        child.release.?(child);
        arrow.ally.destroy(child);
    };

    for (arrays) |*array| {
        var child = try array.produce(n_rows);
        errdefer child.release.?(&child);

        const child_ptr = try arrow.ally.create(arrow.ArrowArray);
        errdefer arrow.ally.destroy(child_ptr);
        child_ptr.* = child;
        array_children.appendAssumeCapacity(child_ptr);
    }

    const array_children_slice = try array_children.toOwnedSlice(arrow.ally);
    errdefer arrow.ally.free(array_children_slice);

    const useless_buffer = try arrow.ally.alloc(?[*]u8, 3);
    errdefer arrow.ally.free(useless_buffer);
    @memset(useless_buffer, null);
    const useless_values = try arrow.ally.alloc(u8, 1);
    errdefer arrow.ally.free(useless_values);
    useless_buffer[1] = useless_values.ptr;

    return .{
        .length = @intCast(n_rows),
        .null_count = 0,
        .buffers = useless_buffer.ptr,
        .n_buffers = 1,
        .n_children = @intCast(arrays.len),
        .children = array_children_slice.ptr,
    };
}
