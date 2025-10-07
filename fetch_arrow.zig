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
const zeit = @import("zeit");

const CDataType = zodbc.odbc.types.CDataType;

const Conversions = enum {
    begin_row,
    end_row,

    wchar,
    sshort,
    ushort,
    slong,
    ulong,
    float,
    double,
    bit,
    stinyint,
    utinyint,
    sbigint,
    ubigint,
    binary,
    numeric,
    guid,
    type_date,
    type_time,
    type_timestamp__second,
    type_timestamp__milli,
    type_timestamp__micro,
    type_timestamp__nano,
    type_timestamp__string,
    ss_timestampoffset__second,
    ss_timestampoffset__milli,
    ss_timestampoffset__micro,
    ss_timestampoffset__nano,
    ss_timestampoffset__string,
    ss_time2__second,
    ss_time2__milli,
    ss_time2__micro,
    ss_time2__nano,

    fn ArrowType(tag: Conversions) type {
        return switch (tag) {
            .wchar => u32,
            .sshort => tag.Type(),
            .ushort => tag.Type(),
            .slong => tag.Type(),
            .ulong => tag.Type(),
            .float => tag.Type(),
            .double => tag.Type(),
            .bit => std.DynamicBitSetUnmanaged.MaskInt,
            .stinyint => tag.Type(),
            .utinyint => tag.Type(),
            .sbigint => tag.Type(),
            .ubigint => tag.Type(),
            .binary => u32,
            .numeric => i128,
            .guid => tag.Type(),
            .type_date => i32,
            .type_time => i32,
            .type_timestamp__second => i64,
            .type_timestamp__milli => i64,
            .type_timestamp__micro => i64,
            .type_timestamp__nano => i64,
            .type_timestamp__string => u32,
            .ss_timestampoffset__second => i64,
            .ss_timestampoffset__milli => i64,
            .ss_timestampoffset__nano => i64,
            .ss_timestampoffset__micro => i64,
            .ss_timestampoffset__string => u32,
            .ss_time2__second => i32,
            .ss_time2__milli => i32,
            .ss_time2__micro => i64,
            .ss_time2__nano => i64,
            .begin_row, .end_row => unreachable,
        };
    }

    fn isVarArrow(tag: Conversions) bool {
        return switch (tag) {
            .wchar, .binary, .ss_timestampoffset__string, .type_timestamp__string => true,
            else => false,
        };
    }

    fn asTypeValue(comptime tag: Conversions, data: []u8) Type(tag) {
        return std.mem.bytesToValue(Type(tag), data);
    }

    fn Type(comptime tag: Conversions) type {
        @setEvalBranchQuota(0xFFFF_FFFF);
        if (tag == .begin_row or tag == .end_row) {
            return void;
        }
        var tok = std.mem.tokenizeSequence(u8, @tagName(tag), "__");
        return std.enums.nameCast(zodbc.odbc.types.CDataType, tok.next().?).Type();
    }
};

const Schema = struct {
    name: [:0]const u8,
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
                    const private_inner: *Private = @ptrCast(@alignCast(schema.private_data));
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
    tag: Conversions,
    ownership_stolen: bool = false,
    n_rows_max: usize,

    inline fn valid(self: *@This()) std.DynamicBitSetUnmanaged {
        return .{ .bit_length = self.n_rows_max, .masks = self.valid_mem.ptr };
    }

    fn init(n_rows: usize, tag: Conversions, allocator: std.mem.Allocator) !@This() {
        const valid_mem = try allocator.alignedAlloc(std.DynamicBitSetUnmanaged.MaskInt, .of(usize), bitSetLen(n_rows));
        errdefer allocator.free(valid_mem);
        @memset(valid_mem, 0);

        switch (tag) {
            .begin_row, .end_row => unreachable,
            inline else => |comp_tag| {
                const T = comp_tag.ArrowType();
                if (comptime comp_tag.isVarArrow()) {
                    comptime std.debug.assert(T == u32);
                    const value = try allocator.alignedAlloc(T, .of(usize), n_rows + 1);
                    errdefer allocator.free(value);
                    value[0] = 0;
                    const data = try allocator.alignedAlloc(u8, .of(usize), n_rows * 42);
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
                    const bitset_mem = try allocator.alignedAlloc(T, .of(usize), bitSetLen(n_rows));
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
                const value = try allocator.alignedAlloc(T, .of(usize), n_rows);
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
                allocator.free(@as([]comp_tag.ArrowType(), @ptrCast(@alignCast(self.value))));
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

cycle: []Conversions,
cols: []Schema,

pub fn init(res: *const zodbc.ResultSet, allocator: std.mem.Allocator, dt7_fetch: Dt7Fetch) !@This() {
    var cols: std.ArrayListUnmanaged(Schema) = try .initCapacity(allocator, res.n_cols);
    defer cols.deinit(allocator);
    defer for (cols.items) |col| col.deinit(allocator);

    const cycle = try allocator.alloc(Conversions, res.n_cols + 1);
    errdefer allocator.free(cycle);
    for (res.columns.items, 0..) |col, i_col| {
        cycle[i_col] = switch (col.c_type) {
            inline else => |c_type| blk_outer: {
                const tag: Conversions, const fmt_raw = switch (c_type) {
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
                            break :blk .{ .type_timestamp__second, "tss:" };
                        } else if (prec <= 3) {
                            break :blk .{ .type_timestamp__milli, "tsm:" };
                        } else if (prec <= 6) {
                            break :blk .{ .type_timestamp__micro, "tsu:" };
                        } else if (prec <= 9) {
                            switch (dt7_fetch) {
                                .micro => break :blk .{ .type_timestamp__micro, "tsu:" },
                                .nano => break :blk .{ .type_timestamp__nano, "tsn:" },
                                .string => break :blk .{ .type_timestamp__string, "u" },
                            }
                        } else {
                            unreachable;
                        }
                    },
                    .ss_timestampoffset => blk: {
                        const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                        std.debug.assert(prec >= 0);
                        if (prec == 0) {
                            break :blk .{ .ss_timestampoffset__second, "tss:+00:00" };
                        } else if (prec <= 3) {
                            break :blk .{ .ss_timestampoffset__milli, "tsm:+00:00" };
                        } else if (prec <= 6) {
                            break :blk .{ .ss_timestampoffset__micro, "tsu:+00:00" };
                        } else if (prec <= 9) {
                            switch (dt7_fetch) {
                                .micro => break :blk .{ .ss_timestampoffset__micro, "tsu:+00:00" },
                                .nano => break :blk .{ .ss_timestampoffset__nano, "tsn:+00:00" },
                                .string => break :blk .{ .ss_timestampoffset__string, "u" },
                            }
                        } else {
                            unreachable;
                        }
                    },
                    .ss_time2 => blk: {
                        const prec = try res.stmt.colAttribute(@intCast(i_col + 1), .precision);
                        std.debug.assert(prec >= 0);
                        if (prec == 0) {
                            break :blk .{ .ss_time2__second, "tts" };
                        } else if (prec <= 3) {
                            break :blk .{ .ss_time2__milli, "ttm" };
                        } else if (prec <= 6) {
                            break :blk .{ .ss_time2__micro, "ttu" };
                        } else if (prec <= 9) {
                            break :blk .{ .ss_time2__nano, "ttn" };
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
                    //         inline 0 => break :blk .{ std.meta.stringToEnum(Conversions, @tagName(c_type) ++ "_second"), prefix ++ "s" ++ suffix },
                    //         inline 7...9 => blk: {
                    //             if (c_type == .ss_time2) {
                    //                 break :blk .{ std.meta.stringToEnum(Conversions, @tagName(c_type) ++ "_nano"), prefix ++ "n" ++ suffix };
                    //             }
                    //             switch (dt7_fetch) {
                    //                 .micro => break :blk .{ std.meta.stringToEnum(Conversions, @tagName(c_type) ++ "_micro"), prefix ++ "u" ++ suffix },
                    //                 .nano => break :blk .{ std.meta.stringToEnum(Conversions, @tagName(c_type) ++ "_nano"), prefix ++ "n" ++ suffix },
                    //                 .string => break :blk .{ std.meta.stringToEnum(Conversions, @tagName(c_type) ++ "_string"), "u" },
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
    sw: switch (Conversions.begin_row) {
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
            const values: []conv.ArrowType() = @ptrCast(@alignCast(arr.value));
            if (res.borrowed_row[i_col]) |bytes| {
                var valid = arr.valid();
                valid.set(i_row);
                if (comptime conv.isVarArrow()) {
                    try odbcToArrowVar(
                        bytes,
                        values,
                        i_row,
                        conv,
                        arr,
                    );
                } else if (comptime conv == .bit) {
                    var bitset: std.DynamicBitSetUnmanaged = .{ .bit_length = n_rows, .masks = values.ptr };
                    switch (conv.asTypeValue(bytes)) {
                        0 => bitset.unset(i_row),
                        1 => bitset.set(i_row),
                        else => unreachable,
                    }
                } else {
                    values[i_row] = try odbcToArrowScalar(bytes, conv);
                }
            } else {
                var valid = arr.valid();
                valid.unset(i_row);
                if (conv.isVarArrow()) {
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

    return .{ batch_schema, batch_array };
}

inline fn odbcToArrowVar(
    bytes: []u8,
    values: []u32,
    i_row: usize,
    comptime conv: Conversions,
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

    const bytes_T: []conv.Type() = @ptrCast(@alignCast(bytes));
    switch (conv) {
        .binary => {
            @memcpy(data[values[i_row] .. values[i_row] + bytes_T.len], bytes_T);
            values[i_row + 1] = values[i_row] + @as(u32, @intCast(bytes_T.len));
        },
        .wchar => {
            const len = std.unicode.wtf16LeToWtf8(data[values[i_row]..], bytes_T);
            values[i_row + 1] = values[i_row] + @as(u32, @intCast(len));
        },
        .ss_timestampoffset__string, .type_timestamp__string => {
            @panic("TODO");
        },
        else => @compileError(@tagName(conv) ++ " is not a variable length type"),
    }
}

inline fn odbcToArrowScalar(
    bytes: []u8,
    comptime conv: Conversions,
) !conv.ArrowType() {
    const val = conv.asTypeValue(bytes);

    switch (conv) {
        .sshort,
        .ushort,
        .slong,
        .ulong,
        .float,
        .double,
        .stinyint,
        .utinyint,
        .sbigint,
        .ubigint,
        => return val,
        .numeric => return @as(i128, @bitCast(val.val)) * switch (val.sign) {
            1 => @as(i2, 1),
            0 => @as(i2, -1),
            else => unreachable,
        },
        .guid => return @bitCast(val),
        .type_date,
        .type_time,
        .type_timestamp__second,
        .type_timestamp__milli,
        .type_timestamp__micro,
        .type_timestamp__nano,
        .ss_timestampoffset__second,
        .ss_timestampoffset__milli,
        .ss_timestampoffset__micro,
        .ss_timestampoffset__nano,
        .ss_time2__second,
        .ss_time2__milli,
        .ss_time2__micro,
        .ss_time2__nano,
        => {
            const T = conv.Type();
            const prec_mult = comptime blk: {
                var tok = std.mem.tokenizeSequence(u8, @tagName(conv), "__");
                _ = tok.next().?;
                const prec_name = tok.next();
                if (prec_name) |n|
                    if (std.mem.eql(u8, n, "second"))
                        break :blk 9
                    else if (std.mem.eql(u8, n, "milli"))
                        break :blk 6
                    else if (std.mem.eql(u8, n, "micro"))
                        break :blk 3
                    else if (std.mem.eql(u8, n, "nano"))
                        break :blk 0
                    else
                        @compileError("Unknown precision name: " ++ n)
                else
                    break :blk null;
            };
            const A = conv.ArrowType();
            var arrow_val: A = 0;
            if (@hasField(T, "year")) {
                arrow_val += @intCast(zeit.daysFromCivil(.{
                    .year = @intCast(val.year),
                    .month = @enumFromInt(val.month),
                    .day = @intCast(val.day),
                }));
            }
            if (@hasField(T, "hour")) {
                arrow_val *= 24 * 3600;
                arrow_val += (0 +
                    @as(A, val.hour) * 3600 +
                    @as(A, val.minute) * 60 +
                    @as(A, val.second));
            }
            if (@hasField(T, "timezone_hour")) {
                arrow_val -= @as(A, val.timezone_hour) * 3600;
                arrow_val -= @as(A, val.timezone_minute) * 60;
            }
            if (@hasField(T, "fraction")) {
                const err = error.@"Datetime(7) value is too large or too small for Arrow type with nano second precision";
                arrow_val = std.math.mul(A, arrow_val, std.math.pow(A, 10, 9 - prec_mult)) catch return err;
                arrow_val = std.math.add(A, arrow_val, @divTrunc(
                    @as(A, @intCast(val.fraction)),
                    std.math.pow(A, 10, prec_mult),
                )) catch return err;
            }
            return arrow_val;
        },
        else => @compileError("Conversion " ++ @tagName(conv) ++ " is not a scalar type"),
    }
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
