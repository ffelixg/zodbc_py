const std = @import("std");
const py = @import("py");
pub const ally = std.heap.raw_c_allocator;

pub const ArrowSchema = extern struct {
    // Array type description
    format: [*:0]const u8,
    name: ?[*:0]const u8,
    metadata: ?[*]const u8 = null,
    flags: i64 = 0,
    n_children: i64 = 0,
    children: ?[*]*ArrowSchema = null,
    dictionary: ?[*]ArrowSchema = null,

    // Release callback
    release: ?*const fn (*ArrowSchema) callconv(.c) void,
    // Opaque producer-specific data
    private_data: ?*anyopaque = null,
};

/// I think this the way this function looks is mostly forced by the "buffer moving" property.
pub fn array_release(arrow_array: *ArrowArray) callconv(.c) void {
    var buffers_freed: usize = 0;
    const buf_s = arrow_array.buffers[0..3];
    for (buf_s) |buf| {
        if (buf) |b| {
            std.c.free(@ptrCast(b));
            buffers_freed += 1;
        }
    }
    std.debug.assert(buffers_freed == arrow_array.n_buffers);
    ally.free(buf_s);

    if (arrow_array.children) |children| {
        for (children[0..@intCast(arrow_array.n_children)]) |child| {
            child.release.?(child);
            ally.destroy(child);
        }
    }

    arrow_array.release = null;
}

pub const ArrowArray = extern struct {
    // Array data description
    length: i64,
    null_count: i64,
    offset: i64 = 0,
    n_buffers: i64,
    n_children: i64 = 0,
    buffers: [*]?[*]u8,
    children: ?[*]*ArrowArray = null,
    dictionary: ?[*]ArrowArray = null,

    // Release callback
    release: ?*const fn (*ArrowArray) callconv(.c) void = array_release,
    // Opaque producer-specific data, must be pointer sized
    private_data: ?*anyopaque = null,
};

pub const SchemaCapsule = py.PyCapsule(ArrowSchema, "arrow_schema", &struct {
    fn deinit(self: *ArrowSchema) callconv(.c) void {
        _ = self;
        // if (self.release) |release|
        //     release(self);
    }
}.deinit);

pub const ArrayCapsule = py.PyCapsule(ArrowArray, "arrow_array", &struct {
    fn deinit(self: *ArrowArray) callconv(.c) void {
        _ = self;
        // if (self.release) |release|
        //     release(self);
    }
}.deinit);
