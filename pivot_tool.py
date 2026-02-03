import sqlite3
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "komunal.db"
SOURCE = "v_pivot_base"


# Spojenie s databázou
def get_conn():
    """Open a connection to the SQLite file."""
    return sqlite3.connect(DB_FILE)


# Načítanie vybraných stĺpcov
def get_columns():
    """Return a list of column names available in v_pivot_base."""
    with get_conn() as conn:
        rows = conn.execute(f"PRAGMA table_info({SOURCE});").fetchall()
    return [r[1] for r in rows]


ALLOWED_COLS = set(get_columns())


def check_cols_exist(cols):
    """Raise a clear error if a user asks for a column that doesn't exist."""
    for c in cols:
        if c not in ALLOWED_COLS:
            raise ValueError(f"Unknown column: {c}. Check spelling/case.")


# Z vybraných filtrov poskladá časť SQL query
def build_where(filters):
    """
    filters: list of dicts:
      {"col": "Rok", "op": "between", "val": (2016, 2018)}
      {"col": "Typ_odpadu", "op": "=", "val": "plasty"}
      {"col": "fldICO_Odberatel", "op": "is null", "val": None}
      {"col": "Rok", "op": "in", "val": [2015, 2016, 2017]}
    Returns: (where_sql, params_list)
    """
    clauses = []
    params = []

    for f in filters:
        col = f["col"]
        op = f["op"].strip().lower()
        val = f.get("val", None)

        check_cols_exist([col])

        col_sql = f'"{col}"'

        if op in ("=", "!=", "<", "<=", ">", ">="):
            clauses.append(f"{col_sql} {op} ?")
            params.append(val)

        elif op == "between":
            a, b = val
            clauses.append(f"{col_sql} BETWEEN ? AND ?")
            params.extend([a, b])

        elif op == "in":
            items = list(val)
            if len(items) == 0:
                clauses.append("1=0")
            else:
                placeholders = ",".join(["?"] * len(items))
                clauses.append(f"{col_sql} IN ({placeholders})")
                params.extend(items)

        elif op == "like":
            clauses.append(f"{col_sql} LIKE ?")
            params.append(val)

        elif op == "is null":
            clauses.append(f"{col_sql} IS NULL")

        elif op == "is not null":
            clauses.append(f"{col_sql} IS NOT NULL")

        else:
            raise ValueError(f"Unsupported operator: {op}")

    if not clauses:
        return "", []

    return "WHERE " + " AND ".join(clauses), params


# Poskladanie celej SQL query podľa filtrov, group by a vybranej measure
def run_pivot(group_by, measure, filters=None, order_by="value", order_desc=True, limit=None):
    """
    group_by: list of columns, e.g. ["Obec", "Rok"]
    measure: one of:
        "sum_amount"  -> SUM(fldMnozstvo)
        "count_rows"  -> COUNT(*)
    filters: list of filter dicts (see build_where)
    """
    filters = filters or []

    check_cols_exist(group_by)

    measure_map = {
        "sum_amount": 'SUM("fldMnozstvo")',
        "avg_amount": 'AVG("fldMnozstvo")',
        "min_amount": 'MIN("fldMnozstvo")',
        "max_amount": 'MAX("fldMnozstvo")',
        "count_rows": "COUNT(*)",
    }

    if measure not in measure_map:
        raise ValueError("Unknown measure")

    measure_sql = measure_map[measure]
    value_name = "value"

    where_sql, params = build_where(filters)

    if group_by:
        gb_sql = ", ".join([f'"{c}"' for c in group_by])
        sql = f"""
        SELECT {gb_sql}, {measure_sql} AS {value_name}
        FROM {SOURCE}
        {where_sql}
        GROUP BY {gb_sql}
        """
    else:
        sql = f"""
        SELECT {measure_sql} AS {value_name}
        FROM {SOURCE}
        {where_sql}
        """

    if order_by is None or order_by == "":
        order_by = "value"

    if order_by == "value":
        order_expr = value_name
    else:
        if order_by not in group_by:
            raise ValueError("order_by must be 'value' or one of the group_by columns")
        order_expr = f'"{order_by}"'

    sql += f'\nORDER BY {order_expr} {"DESC" if order_desc else "ASC"}'

    if limit is not None:
        sql += "\nLIMIT ?"
        params.append(int(limit))



    print("\n--- SQL generated ---")
    print(sql.strip())
    print("Params:", params)

    with get_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    return df


# Funkcie na export tabuľky
def export_excel(df, out_file="pivot_export.xlsx", sheet_name="pivot"):
    out_path = BASE_DIR / out_file
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    print("Saved Excel:", out_path)


def export_csv(df, out_file="pivot_export.csv"):
    out_path = BASE_DIR / out_file
    df.to_csv(out_path, index=False)
    print("Saved CSV:", out_path)

if __name__ == "__main__":
    group_by = []

    filters = []

    df = run_pivot(group_by=group_by, measure="sum_amount", filters=filters)

    print("\nRows:", len(df))
    print(df.head(10))

    export_excel(df, out_file="pivot_export.xlsx")
    # export_csv(df, out_file="pivot_export.csv")
