class ArrowTVPType:
    def from_name(table_name: str, schema_name: str | None = None) -> "ArrowTVP":
        self = ArrowTVPType()
        self.table_name = table_name
        self.schema_name = schema_name
        return self

class ArrowTVP:
    def __init__(self, T: ArrowTVPType, batch: "pyarrow.RecordBatch"):
        self._type = T
        self._batch = batch
        self._batch_schema, self._batch_array = batch.__arrow_c_array__()
