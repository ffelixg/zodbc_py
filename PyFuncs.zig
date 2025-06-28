const c = @import("c");
const Obj = *c.PyObject;

cls_datetime: Obj,
cls_date: Obj,
cls_time: Obj,
cls_timezone: Obj,
cls_timedelta: Obj,
cls_decimal: Obj,
cls_uuid: Obj,

pub fn init() !@This() {
    const mod_datetime = c.PyImport_ImportModule("datetime") orelse return error.PyErr;
    defer c.Py_DECREF(mod_datetime);
    const cls_datetime = c.PyObject_GetAttrString(mod_datetime, "datetime") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_datetime);
    const cls_date = c.PyObject_GetAttrString(mod_datetime, "date") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_date);
    const cls_time = c.PyObject_GetAttrString(mod_datetime, "time") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_time);
    const cls_timezone = c.PyObject_GetAttrString(mod_datetime, "timezone") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_timezone);
    const cls_timedelta = c.PyObject_GetAttrString(mod_datetime, "timedelta") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_timedelta);

    const mod_decimal = c.PyImport_ImportModule("decimal") orelse return error.PyErr;
    defer c.Py_DECREF(mod_decimal);
    const cls_decimal = c.PyObject_GetAttrString(mod_decimal, "Decimal") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_decimal);

    const mod_uuid = c.PyImport_ImportModule("uuid") orelse return error.PyErr;
    defer c.Py_DECREF(mod_uuid);
    const cls_uuid = c.PyObject_GetAttrString(mod_uuid, "UUID") orelse return error.PyErr;
    errdefer c.Py_DECREF(cls_uuid);

    return .{
        .cls_datetime = cls_datetime,
        .cls_date = cls_date,
        .cls_time = cls_time,
        .cls_timezone = cls_timezone,
        .cls_timedelta = cls_timedelta,
        .cls_decimal = cls_decimal,
        .cls_uuid = cls_uuid,
    };
}

pub fn deinit(self: @This()) void {
    c.Py_DECREF(self.cls_datetime);
    c.Py_DECREF(self.cls_date);
    c.Py_DECREF(self.cls_time);
    c.Py_DECREF(self.cls_timezone);
    c.Py_DECREF(self.cls_timedelta);
    c.Py_DECREF(self.cls_decimal);
    c.Py_DECREF(self.cls_uuid);
}
