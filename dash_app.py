# Dash appka s pivotnou tabuľkou
# Niektoré funkcie pochádzajú zo súboru pivot_tool.py, mali by sme ho preto mať v rovnakom priečinku

from io import StringIO, BytesIO
import pandas as pd
from dash import Dash, dcc, html, dash_table, Input, Output, State, ALL, ctx
from dash.dcc import send_string, send_bytes
from pivot_tool import run_pivot, get_columns

app = Dash(__name__)
app.title = "Waste Pivot (Dash)"

ALL_COLS = get_columns()

OPS = ["=", "!=", "<", "<=", ">", ">=", "IN", "BETWEEN", "LIKE", "IS NULL", "IS NOT NULL"]

MEASURES = {
    "Sum of amount (fldMnozstvo)": "sum_amount",
    "Average of amount (fldMnozstvo)": "avg_amount",
    "Min of amount (fldMnozstvo)": "min_amount",
    "Max of amount (fldMnozstvo)": "max_amount",
    "Count of rows": "count_rows",
}


def filter_row(i: int):
    """
    Creates one horizontal row with:
      - column dropdown
      - operator dropdown
      - value input
      - remove button
    The IDs use pattern-matching (type/index), so we can have many rows.
    """
    return html.Div(
        style={"display": "flex", "gap": "8px", "alignItems": "center", "marginBottom": "6px"},
        children=[
            dcc.Dropdown(
                id={"type": "f_col", "index": i},
                options=[{"label": c, "value": c} for c in ALL_COLS],
                placeholder="Column",
                style={"width": "280px"},
                clearable=True,
            ),
            dcc.Dropdown(
                id={"type": "f_op", "index": i},
                options=[{"label": op, "value": op} for op in OPS],
                value="=",
                style={"width": "140px"},
                clearable=False,
            ),
            dcc.Input(
                id={"type": "f_val", "index": i},
                type="text",
                placeholder="Value (IN/BETWEEN: comma-separated)",
                style={"width": "360px"},
            ),
            html.Button("Remove", id={"type": "f_rm", "index": i}, n_clicks=0),
        ],
    )


# Nastavíme výzor aplikácie, zatiaľ je to len pracovná verzia
app.layout = html.Div(
    style={"fontFamily": "Arial", "padding": "12px"},
    children=[
        html.H2("Waste Pivot Table + option to export"),

        html.Div("Group by:"),
        dcc.Dropdown(
            id="group_by",
            options=[{"label": c, "value": c} for c in ALL_COLS],
            multi=True,
            value=["Obec", "Rok"],
        ),

        html.Br(),

        html.Div("Measure:"),
        dcc.Dropdown(
            id="measure",
            options=[{"label": k, "value": v} for k, v in MEASURES.items()],
            value="sum_amount",
            clearable=False,
            style={"width": "360px"},
        ),

        html.Br(),
        html.Div("Order by:"),
        dcc.Dropdown(
            id="order_by",
            options=[{"label": "value (aggregated result)", "value": "value"}],
            value="value",
            clearable=False,
            style={"width": "360px"},
        ),

        html.Br(),
        html.Div("Order direction:"),
        dcc.Dropdown(
            id="order_dir",
            options=[
                {"label": "Descending", "value": "desc"},
                {"label": "Ascending", "value": "asc"},
            ],
            value="desc",
            clearable=False,
            style={"width": "200px"},
        ),


        html.Hr(),

        html.H4("Filters (optional)"),
        html.Div(id="filters_container", children=[filter_row(0)]),
        html.Button("Add filter", id="add_filter", n_clicks=0),

        html.Hr(),

        html.Div(style={"display": "flex", "gap": "8px"}, children=[
            html.Button("Run", id="run", n_clicks=0),
            html.Button("Download CSV", id="download_csv_btn", n_clicks=0),
            html.Button("Download XLSX", id="download_xlsx_btn", n_clicks=0),
            dcc.Download(id="download"),
        ]),

        html.Div(id="info", style={"marginTop": "10px"}),

        dash_table.DataTable(
            id="table",
            page_size=20,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "6px", "maxWidth": "300px", "whiteSpace": "normal"},
        ),

        dcc.Store(id="filters_state"),

        dcc.Store(id="latest_query"),
    ],
)

# Zmení čísla na integers alebo floats, ostatné hodnoty nechá ako strings
def maybe_number(x: str):
    """
    Try to convert a string to int or float.
    If it doesn't look like a number, return the original string.
    """
    s = str(x).strip()

    # try int first
    try:
        if "." not in s:
            return int(s)
    except ValueError:
        pass

    # try float
    try:
        return float(s)
    except ValueError:
        return s


# Funkcia, ktorá spracuje filter v tabuľke
def parse_filter(col, op, val_text):
    if not col:
        return None

    op_norm = (op or "").strip().lower()

    if op_norm in ("is null", "is not null"):
        return {"col": col, "op": op_norm}

    if val_text is None or str(val_text).strip() == "":
        return None

    s = str(val_text).strip()

    if op_norm == "in":
        items = [x.strip() for x in s.split(",") if x.strip() != ""]
        items = [maybe_number(x) for x in items] 
        return {"col": col, "op": "in", "val": items}

    if op_norm == "between":
        parts = [x.strip() for x in s.split(",")]
        if len(parts) != 2:
            return None
        a = maybe_number(parts[0])              
        b = maybe_number(parts[1])                
        return {"col": col, "op": "between", "val": (a, b)}

    if op_norm == "like":
        return {"col": col, "op": "like", "val": s}

    # default: simple operators like =, >, <= ...
    return {"col": col, "op": op_norm, "val": maybe_number(s)}



# Pridávanie a odoberanie filtrov pomocou tlačidiel
@app.callback(
    Output("filters_container", "children"),
    Input("add_filter", "n_clicks"),
    Input({"type": "f_rm", "index": ALL}, "n_clicks"),
    State("filters_container", "children"),
    prevent_initial_call=True,
)
def update_filter_rows(add_clicks, remove_clicks, children):
    triggered = ctx.triggered_id

    # Pridanie filtra
    if triggered == "add_filter":
        new_index = len(children)
        children.append(filter_row(new_index))
        return children

    # Odobranie filtra
    if isinstance(triggered, dict) and triggered.get("type") == "f_rm":
        rm_index = triggered.get("index")

        kept_positions = [i for i in range(len(children)) if i != rm_index]
        if not kept_positions:
            return [filter_row(0)]

        new_children = []
        for new_i, _old_i in enumerate(kept_positions):
            new_children.append(filter_row(new_i))
        return new_children

    return children

@app.callback(
    Output("filters_state", "data"),
    Input({"type": "f_col", "index": ALL}, "value"),
    Input({"type": "f_op", "index": ALL}, "value"),
    Input({"type": "f_val", "index": ALL}, "value"),
)
def collect_filters(cols, ops, vals):
    filters = []
    for c, o, v in zip(cols, ops, vals):
        f = parse_filter(c, o, v)
        if f is not None:
            filters.append(f)
    return filters

# Zariadi usporiadanie riadkov podľa výberu
@app.callback(
    Output("order_by", "options"),
    Output("order_by", "value"),
    Input("group_by", "value"),
    State("order_by", "value"),
)
def update_order_by_options(group_by, current_value):
    group_by = group_by or []
    opts = [{"label": "value (aggregated result)", "value": "value"}]
    opts += [{"label": c, "value": c} for c in group_by]

    valid_values = [o["value"] for o in opts]
    if current_value not in valid_values:
        current_value = "value"

    return opts, current_value

# Spustenie query v SQL formáte
@app.callback(
    Output("table", "data"),
    Output("table", "columns"),
    Output("info", "children"),
    Output("latest_query", "data"),
    Input("run", "n_clicks"),
    State("group_by", "value"),
    State("measure", "value"),
    State("filters_state", "data"),
    State("order_by", "value"),
    State("order_dir", "value"),
)
def run_query(n_clicks, group_by, measure, filters_state, order_by, order_dir):
    if n_clicks == 0:
        return [], [], "Choose settings and click Run.", None

    group_by = group_by or []
    filters_state = filters_state or []

    PREVIEW_LIMIT = 200
    order_desc = (order_dir == "desc")

    df = run_pivot(
        group_by=group_by,
        measure=measure,
        filters=filters_state,
        order_by=order_by,
        order_desc=order_desc,
        limit=PREVIEW_LIMIT,
    )

    columns = [{"name": c, "id": c} for c in df.columns]
    data = df.to_dict("records")

    info = f"Showing {len(df)} rows (limited to {PREVIEW_LIMIT}). | Measure: {measure} | Filters: {len(filters_state)}"

    query_state = {
        "group_by": group_by,
        "measure": measure,
        "filters": filters_state,
        "order_by": order_by,
        "order_desc": order_desc
    }

    return data, columns, info, query_state


# Sťahovanie tabuliek v .csv a .xlsx formátoch
@app.callback(
    Output("download", "data"),
    Input("download_csv_btn", "n_clicks"),
    Input("download_xlsx_btn", "n_clicks"),
    State("latest_query", "data"),
    prevent_initial_call=True,
)
def download_file(n_csv, n_xlsx, query_state):
    if not query_state:
        return None  # najprv musíme stlačiť "Run" aby sa dala tabuľka stiahnuť

    df = run_pivot(
        group_by=query_state["group_by"],
        measure=query_state["measure"],
        filters=query_state["filters"],
        order_by=query_state.get("order_by", "value"),
        order_desc=query_state.get("order_desc", True),
    )


    trigger = ctx.triggered_id

    if trigger == "download_csv_btn":
        buf = StringIO()
        df.to_csv(buf, index=False)
        return send_string(buf.getvalue(), filename="pivot_export.csv")

    if trigger == "download_xlsx_btn":
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="pivot")
        bio.seek(0)
        return send_bytes(bio.getvalue(), filename="pivot_export.xlsx")

    return None


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8866, debug=False)



