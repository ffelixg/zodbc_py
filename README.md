# zodbc

This odbc client tries to be compatible with the DB API 2 Specification, while also treating pyarrow compatibility as a first class citizen. At the moment it is tested with the msodbc driver. Compilation and installation is made easy by ziglang being a simple build dependency. The only system dependencies for compilation should be development headers for unixodbc (if on linux) and python.


# Installation

```bash
pip install zodbc
```


# Features


## DB API 2

```py
import zodbc
import os

con = zodbc.connect(os.environ["ODBC_CONSTR"])  # Connect with odbc connection string or DSN
cur = con.cursor()  # Cursor, which translates to an odbc statement handle

# python parameters & return values, data types like str, int, bool, UUID, decimal, date, time, datetime
cur.execute("select ? a", [42])
assert cur.fetchone()[0] == 42

# Short hand alternative for getting column names from cur.description
assert [c[0] for c in cur.description] == cur.column_names == ["a"]

con.commit()  # Transactions, autocommit is disabled by default
```


## Additional fetch methods

The DB API 2 fetch methods `fetchone`, `fetchmany` and `fetchall` return (lists of) tuples. Additionally, there is `fetch_named` which returns a list of named tuples, which allows accessing fields by attribute name like `row.a` as well as `fetch_dict` which returns a list of dicts. `fetch_dict` requires unique column names in the query. These methods are implemented directly in Zig and should therefore be more efficient than fetching tuples and converting them in python.


## Apache Arrow

A query can be executed for every row in an Arrow RecordBatch.

```py
import pyarrow as pa

cur.execute("drop table if exists t1")
cur.execute("create table t1(a int)")
cur.executemany_arrow(
    "insert into t1(a) values(?)",
    pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["a"])
)
cur.execute("select * from t1").fetchall()  # [(1,), (2,), (3,)]
```

Arrow RecordBatches can be fetched from a query result.

```py
cur.execute("select 1 a").fetch_arrow()
```

Arrow RecordBatches can be used as a table valued parameter for MS SQL.

```py
cur.execute("drop type if exists test_tabletype")
cur.execute("create type test_tabletype as table(a int)")
con.commit()

assert cur.execute(
    "select sum(a) from ? where a <= ?",
    [
        zodbc.ArrowTVP(
            zodbc.ArrowTVPType.from_name("test_tabletype"),
            pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["a"]),
        ),
        2,
    ]
).fetchone()[0] == 3
```


# Differences with pyodbc

- UUID is always returned as a python UUID object (no pyodbc.native_uuid global variable)
- No output conversion (add_output_converter connection method)
- More explicit:
    - No `execute(query, param1, param2)` => params must be passed as sequence
    - No Cursor.commit, instead use Cursor.connection.commit
- Connection.getinfo takes a string instead of the odbc enum value
