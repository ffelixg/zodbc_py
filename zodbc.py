import _zodbc
import typing
from enum import IntEnum

class Datetime2_7_Fetch(IntEnum):
    micro = 1
    string = 2
    nano = 3

class Connection:
    def __init__(self, constr: str):
        self._con = _zodbc.connect(constr)
    
    @property
    def autocommit(self) -> bool:
        return _zodbc.getAutocommit(self._con)
    
    @autocommit.setter
    def autocommit(self, value: bool):
        _zodbc.setAutocommit(self._con, value)

    def cursor(self, datetime2_7_fetch: Datetime2_7_Fetch = Datetime2_7_Fetch.micro) -> "Cursor":
        """
        Create a new cursor object.
        """
        return Cursor(self, datetime2_7_fetch)
    
    def getinfo(self, info_type: str) -> str:
        return _zodbc.getinfo(self._con, info_type)

    def commit(self):
        _zodbc.commit(self._con)    

    def rollback(self):
        _zodbc.rollback(self._con)

    @property
    def closed(self) -> bool:
        if self._con is None:
            return True
        return _zodbc.con_closed(self._con)

    def close(self):
        _zodbc.con_close(self._con)
        self._con = None

def connect(constr: str) -> Connection:
    """
    Connect to a database using the given connection string.
    """
    return Connection(constr)

class Cursor:
    def __init__(self, con: Connection, datetime2_7_fetch: Datetime2_7_Fetch = Datetime2_7_Fetch.micro):
        self._con = con
        self._cursor = _zodbc.cursor(con._con, datetime2_7_fetch)

    def close(self):
        _zodbc.cur_deinit(self._cursor)
        self._cursor = None

    def execute(self, query: str, *args: typing.Any) -> "Cursor":
    # def execute(self, query: str, params: typing.Sequence[typing.Any] = ()) -> "Cursor":
        """
        Execute a SQL query.
        """
        # pyodbc compatibility
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            params = args[0]
        else:
            params = args
        _zodbc.execute(self._cursor, query, params)
        return self

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

    def fetchtuples(self, n: int | None = None) -> list[tuple[typing.Any]]:
        return _zodbc.fetchmany(self._cursor, n)

    def fetchdicts(self, n: int | None = None) -> list[dict[str, typing.Any]]:
        return _zodbc.fetchdicts(self._cursor, n)

    def fetchnamed(self, n: int | None = None) -> list[typing.Any]:
        return _zodbc.fetchnamed(self._cursor, n)

    def fetchmany(self, n: int | None = None) -> list[tuple[typing.Any]]:
        return _zodbc.fetchmany(self._cursor, n)

    def fetchone(self) -> tuple[typing.Any]:
        one = self.fetchmany(1)
        if one:
            return one[0]
        else:
            return None

    def fetchval(self) -> typing.Any:
        # pyodbc compatibility
        one = self.fetchmany(1)
        if one:
            return one[0][0]
        else:
            return None

    def fetchall(self) -> list[tuple[typing.Any]]:
        return self.fetchmany()

    def __iter__(self):
        return self

    def __next__(self) -> tuple[typing.Any]:
        if row := self.fetchone():
            return row
        else:
            raise StopIteration

    def nextset(self) -> bool:
        return _zodbc.nextset(self._cursor)