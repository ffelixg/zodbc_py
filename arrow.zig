const zodbc_test = @import("zodbc_test");
const zodbc = zodbc_test.zodbc;
const State = zodbc_test.State;
const fetch_arrow = zodbc_test.fetch_arrow;
const arrow = zodbc_test.arrow;

const SchemaCapsule = py.PyCapsule(arrow.ArrowSchema, "arrow_schema", &struct {
    fn deinit(self: *arrow.ArrowSchema) callconv(.c) void {
        if (self.release) |release|
            release(self);
    }
}.deinit);
const ArrayCapsule = py.PyCapsule(arrow.ArrowArray, "arrow_array", &struct {
    fn deinit(self: *arrow.ArrowArray) callconv(.c) void {
        if (self.release) |release|
            release(self);
    }
}.deinit);

pub fn arrow_batch(cur_obj: Obj, n_rows: usize) !struct { Obj, Obj } {
    const cur = try StmtCapsule.read_capsule(cur_obj);
    if (cur.result_set == null) {
        cur.result_set = try .init(cur.stmt, std.heap.c_allocator);
    }

    const schema, const array = try fetch_arrow.fetch_arrow_chunk(
        &cur.result_set.?,
        n_rows,
    );

    return .{
        try SchemaCapsule.create_capsule(schema),
        try ArrayCapsule.create_capsule(array),
    };
}
