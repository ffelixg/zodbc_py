class ArrowTVPType:
    def from_name(table: str, schema: str | None = None) -> "ArrowTVP":
        self = ArrowTVPType()
        if schema is None:
            self.name = table
        else:
            self.name = f"{schema}.{table}"
        return self

class ArrowTVP:
    def __init__(self, T: ArrowTVPType, data: "pyarrow.RecordBatch"):
        self._type = T
        self._data = data
