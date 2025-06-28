import _zodbc
import typing

class Connection:
    def __init__(self, constr: str):
        self._con = _zodbc.connect(constr)
    
    @property
    def autocommit(self) -> bool:
        return _zodbc.getAutocommit(self._con)
    
    @autocommit.setter
    def autocommit(self, value: bool):
        _zodbc.setAutocommit(self._con, value)

    def cursor(self) -> "Cursor":
        """
        Create a new cursor object.
        """
        return Cursor(self)

def connect(constr: str) -> Connection:
    """
    Connect to a database using the given connection string.
    """
    return Connection(constr)

class Cursor:
    def __init__(self, con: Connection):
        self._con = con
        self._cursor = _zodbc.cursor(con._con)

    def execute(self, query: str):
        """
        Execute a SQL query.
        """
        _zodbc.execute(self._cursor, query)

    def arrow_batch(self, n_rows: int) -> "pyarrow.RecordBatch":
        import pyarrow
        return pyarrow.RecordBatch._import_from_c_capsule(*_zodbc.arrow_batch(self._cursor, n_rows))
    
    def arrow(self, batch_size: int = 1_000_000) -> "pyarrow.Table":
        import pyarrow

        assert batch_size > 0
        batches = []
        while True:
            batch = self.arrow_batch(batch_size)
            if batch.num_rows < batch_size:
                if not batches or batch.num_rows > 0:
                    batches.append(batch)
                break
            batches.append(batch)
        return pyarrow.Table.from_batches(batches, schema=batches[0].schema)

    def fetchmany(self, n: int) -> list[tuple]:
        return list(zip(*self.arrow_batch(n).to_pydict().values()))

    def fetch_many(self, n: int | None = None) -> list[tuple]:
        return _zodbc.fetch_many(self._cursor, n)

    def fetch_dicts(self, n: int | None = None) -> list[dict[str, typing.Any]]:
        return _zodbc.fetch_dicts(self._cursor, n)

    def fetch_named(self, n: int | None = None) -> list[dict[str, typing.Any]]:
        return _zodbc.fetch_named(self._cursor, n)

    def records(self, n: int | None = None) -> list[dict]:
        assert n is None or n >= 0
        if n is None:
            return self.arrow().to_pylist()
        else:
            return self.arrow_batch(n).to_pylist()
