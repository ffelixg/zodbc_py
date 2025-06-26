import _zodbc

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

    def records(self, n: int | None = None) -> list[dict]:
        assert n is None or n >= 0
        if n is None:
            return self.arrow().to_pylist()
        else:
            return self.arrow_batch(n).to_pylist()


if __name__ == "__main__":
    import os

    con = connect(os.environ["ODBC_CONSTR"])
    print(con.autocommit)
    con.autocommit = False
    print(con.autocommit)
    con.autocommit = True
    print(con.autocommit)
    con.autocommit = False
    print(con.autocommit)
    con.autocommit = True
    print(con.autocommit)

    cur = con.cursor()
    cur.execute("drop table if exists testping")
    # cur.execute("create table testping(id int, name varchar(255))")
    # cur.execute("insert into testping values(1, 'test')")
    cur.execute("create table testping(id int, name varchar(255), dec decimal(4, 2))")
    cur.execute("insert into testping values(1, 'test', 1.23)")
    cur.execute("select * from testping")
    # cur.execute("select top 999 row_number() over(order by (select null)) a, testping.* from testping cross join sys.objects")
    # cur.execute("select * from sys.objects")

    import gc
    # gc.disable()
    print(cur.arrow_batch(1))
    # print(cur.arrow_batch(0))
    print(cur.fetchmany(3))
    # print(cur.fetchmany(2))
    # print(cur.arrow_batch(2))
    print(cur.arrow())
    # print(cur.records())
    
    # cur = con.cursor()
    # cur.execute("select * from test")
    # print(cur.arrow())
