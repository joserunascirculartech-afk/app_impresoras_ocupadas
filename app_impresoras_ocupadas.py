import os
import re
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

# Opcional recomendado para DataFrame <-> Sheets
try:
    from gspread_dataframe import get_as_dataframe, set_with_dataframe
except Exception:
    get_as_dataframe = None
    set_with_dataframe = None


# =========================================================
# CONFIG
# =========================================================
DEFAULT_SHEET_ID = "1OyCDNOH40rI2xZ2BrXCNC-PlrLzv-AV__QhKCnVxcJI"
DEFAULT_JSON_FILENAME = "app-impresoras-ocupadas-b5565c6f9b04.json"

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TABS = ["Pedidos", "Items_Pedido", "Impresoras", "Calendario_Asignaciones", "Feriados"]

# Columnas mínimas para mantener consistencia operacional
REQUIRED_COLS = {
    "Pedidos": ["Pedido_ID"],
    "Items_Pedido": ["Pedido_ID", "Pieza", "Tiempo_por_pieza_min", "Cantidad"],
    "Impresoras": ["Impresora_ID"],
    "Calendario_Asignaciones": ["Fecha", "Impresora_ID", "Pedido_ID"],
    "Feriados": ["Fecha"],
}

DATE_COLS = {
    "Pedidos": ["Fecha_ingreso", "Fecha_inicio", "Fecha_compromiso"],
    "Calendario_Asignaciones": ["Fecha"],
    "Feriados": ["Fecha"],
}

NUM_COLS = {
    "Pedidos": ["Total_min", "Total_horas"],
    "Items_Pedido": ["Tiempo_por_pieza_min", "Cantidad", "Total_item_min", "Impresoras_asignadas", "Horas_dia"],
    "Impresoras": ["Capacidad_horas_dia"],
    "Calendario_Asignaciones": ["Horas_asignadas"],
    "Feriados": ["Factor_capacidad"],
}

st.set_page_config(page_title="CICLA 3D - Tablero", layout="wide")


# =========================================================
# HELPERS
# =========================================================
def parse_sheet_id(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    # ID directo
    if "/" not in value and len(value) > 20:
        return value
    # URL
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return m.group(1) if m else ""

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
    df = normalize_columns(df)
    df = df.dropna(how="all")
    return df

def coerce_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.date
    return out

def coerce_nums(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


# =========================================================
# AUTH / CONNECT
# =========================================================
def build_creds_from_secrets_or_file(json_path: str) -> Credentials:
    """
    Prioridad:
    1) st.secrets["gcp_service_account"] (Streamlit Cloud)
    2) Archivo JSON local (desarrollo)
    """
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)

    # Fallback local
    if not json_path or not os.path.exists(json_path):
        st.error(
            "No se encontraron credenciales.\n\n"
            "En Streamlit Cloud debes configurar Secrets con [gcp_service_account].\n"
            "En local, pon el JSON en la carpeta y verifica el nombre/ruta."
        )
        st.stop()

    return Credentials.from_service_account_file(json_path, scopes=SCOPE)

def connect(sheet_id: str, json_path: str):
    if not sheet_id:
        st.error("Falta SPREADSHEET_ID (en Secrets o en la barra lateral).")
        st.stop()

    creds = build_creds_from_secrets_or_file(json_path)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def ensure_tabs(sh):
    missing = []
    for tab in TABS:
        try:
            sh.worksheet(tab)
        except Exception:
            missing.append(tab)
    if missing:
        st.error("Faltan pestañas requeridas en el Google Sheet: " + ", ".join(missing))
        st.stop()

def sheet_load_tab(sh, tab: str) -> pd.DataFrame:
    ws = sh.worksheet(tab)
    if get_as_dataframe is None:
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)
    else:
        df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
        df = df.dropna(how="all")
    df = clean_df(df)

    if tab in DATE_COLS:
        df = coerce_dates(df, DATE_COLS[tab])
    if tab in NUM_COLS:
        df = coerce_nums(df, NUM_COLS[tab])

    return df

def sheet_save_tab(sh, tab: str, df: pd.DataFrame):
    ws = sh.worksheet(tab)
    df = clean_df(df)

    # Coerciones antes de guardar
    if tab in DATE_COLS:
        df = coerce_dates(df, DATE_COLS[tab])
    if tab in NUM_COLS:
        df = coerce_nums(df, NUM_COLS[tab])

    if set_with_dataframe is None:
        ws.clear()
        rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        ws.update(rows)
    else:
        ws.clear()
        set_with_dataframe(ws, df, include_index=False, include_column_header=True)

@st.cache_data(ttl=30)
def load_all(sheet_id: str, json_path: str) -> dict[str, pd.DataFrame]:
    sh = connect(sheet_id, json_path)
    ensure_tabs(sh)
    out = {}
    for tab in TABS:
        out[tab] = sheet_load_tab(sh, tab)
    return out


# =========================================================
# VALIDATIONS
# =========================================================
def validate_tab(tab: str, df: pd.DataFrame) -> tuple[bool, str]:
    needed = REQUIRED_COLS.get(tab, [])
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return False, f"Faltan columnas requeridas en '{tab}': {', '.join(missing)}"

    # Reglas por tabla
    if tab == "Pedidos":
        ids = df["Pedido_ID"].astype(str).str.strip()
        if (ids == "").any():
            return False, "En 'Pedidos' hay filas con Pedido_ID vacío."
        if ids.duplicated().any():
            dups = ids[ids.duplicated()].unique().tolist()
            return False, f"En 'Pedidos' hay Pedido_ID duplicados: {dups}"

    if tab == "Impresoras":
        ids = df["Impresora_ID"].astype(str).str.strip()
        if (ids == "").any():
            return False, "En 'Impresoras' hay filas con Impresora_ID vacío."
        if ids.duplicated().any():
            dups = ids[ids.duplicated()].unique().tolist()
            return False, f"En 'Impresoras' hay Impresora_ID duplicados: {dups}"

    if tab == "Items_Pedido":
        pid = df["Pedido_ID"].astype(str).str.strip()
        if (pid == "").any():
            return False, "En 'Items_Pedido' hay filas con Pedido_ID vacío."
        for c in ["Tiempo_por_pieza_min", "Cantidad"]:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce")
                if (v < 0).any():
                    return False, f"En 'Items_Pedido', '{c}' tiene valores negativos."

    if tab == "Calendario_Asignaciones":
        fechas = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
        if fechas.isna().any():
            return False, "En 'Calendario_Asignaciones' hay filas con Fecha inválida/vacía."
        for c in ["Impresora_ID", "Pedido_ID"]:
            s = df[c].astype(str).str.strip()
            if (s == "").any():
                return False, f"En 'Calendario_Asignaciones' hay filas con {c} vacío."

    if tab == "Feriados":
        fechas = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
        if fechas.isna().any():
            return False, "En 'Feriados' hay filas con Fecha inválida/vacía."
        if "Factor_capacidad" in df.columns:
            fc = pd.to_numeric(df["Factor_capacidad"], errors="coerce").dropna()
            if ((fc < 0) | (fc > 1)).any():
                return False, "En 'Feriados', Factor_capacidad debe estar entre 0 y 1."

    return True, "OK"


# =========================================================
# COMPUTES (resumen)
# =========================================================
def compute_totals(pedidos: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    p = pedidos.copy()
    it = items.copy()

    if not it.empty:
        if "Total_item_min" not in it.columns and {"Tiempo_por_pieza_min", "Cantidad"}.issubset(it.columns):
            it["Total_item_min"] = (
                pd.to_numeric(it["Tiempo_por_pieza_min"], errors="coerce")
                * pd.to_numeric(it["Cantidad"], errors="coerce")
            )

    if (not it.empty) and {"Pedido_ID", "Total_item_min"}.issubset(it.columns) and ("Pedido_ID" in p.columns):
        agg = it.groupby("Pedido_ID", dropna=False)["Total_item_min"].sum().reset_index()
        p = p.merge(agg, on="Pedido_ID", how="left")
        p["Total_min"] = pd.to_numeric(p.get("Total_min"), errors="coerce")
        p["Total_min"] = p["Total_min"].fillna(p["Total_item_min"])
        p["Total_horas"] = p["Total_min"] / 60

    return p

def build_calendar_pivot(cal: pd.DataFrame) -> pd.DataFrame:
    if cal.empty:
        return pd.DataFrame()
    df = cal.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
    df["Impresora_ID"] = df["Impresora_ID"].astype(str)
    pivot = df.pivot_table(index="Fecha", columns="Impresora_ID", values="Pedido_ID", aggfunc="first")
    return pivot.sort_index()


# =========================================================
# UI
# =========================================================
st.title("CICLA 3D - Tablero de Producción")

# Defaults desde Secrets si existen
default_sheet_from_secrets = st.secrets.get("SPREADSHEET_ID", DEFAULT_SHEET_ID) if hasattr(st, "secrets") else DEFAULT_SHEET_ID

with st.sidebar:
    st.header("Conexión")

    sheet_in = st.text_input("Google Sheet (URL o ID)", value=default_sheet_from_secrets)
    sheet_id = parse_sheet_id(sheet_in) or default_sheet_from_secrets

    # En Cloud no necesitas esto, pero sirve en local
    json_in = st.text_input("JSON local (solo dev)", value=DEFAULT_JSON_FILENAME)
    json_path = json_in.strip()

    if st.button("Recargar datos"):
        load_all.clear()

tables = load_all(sheet_id, json_path)

pedidos = tables["Pedidos"]
items = tables["Items_Pedido"]
impresoras = tables["Impresoras"]
cal = tables["Calendario_Asignaciones"]
feriados = tables["Feriados"]

pedidos_calc = compute_totals(pedidos, items)

tab_resumen, tab_ped, tab_items, tab_imp, tab_cal, tab_fer = st.tabs(
    ["Resumen", "Editar Pedidos", "Editar Items", "Editar Impresoras", "Editar Calendario", "Editar Feriados"]
)

with tab_resumen:
    dfp = pedidos_calc.copy()

    total_pedidos = len(dfp)
    en_prod = int((dfp["Estado"] == "En producción").sum()) if "Estado" in dfp.columns else 0
    pendientes = int((dfp["Estado"] == "Pendiente").sum()) if "Estado" in dfp.columns else 0
    terminados = int((dfp["Estado"] == "Terminado").sum()) if "Estado" in dfp.columns else 0
    total_horas = float(pd.to_numeric(dfp.get("Total_horas", 0), errors="coerce").fillna(0).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pedidos (total)", total_pedidos)
    c2.metric("En producción", en_prod)
    c3.metric("Pendientes", pendientes)
    c4.metric("Horas estimadas (total)", round(total_horas, 1))

    st.subheader("Calendario (vista tipo Distribución)")
    st.dataframe(build_calendar_pivot(cal), use_container_width=True, height=520)

    st.subheader("Pedidos (con totales calculados)")
    st.dataframe(dfp, use_container_width=True)


def editor_block(tab_name: str, df: pd.DataFrame, key: str):
    st.subheader(tab_name)

    edited = st.data_editor(
        df,
        key=key,
        use_container_width=True,
        num_rows="dynamic",
    )

    # Coerciones para consistencia
    if tab_name in DATE_COLS:
        edited = coerce_dates(edited, DATE_COLS[tab_name])
    if tab_name in NUM_COLS:
        edited = coerce_nums(edited, NUM_COLS[tab_name])

    ok, msg = validate_tab(tab_name, edited)

    c1, c2, c3 = st.columns([1, 2, 3])

    with c1:
        if st.button(f"Guardar {tab_name}", disabled=not ok):
            sh = connect(sheet_id, json_path)
            ensure_tabs(sh)
            sheet_save_tab(sh, tab_name, edited)
            load_all.clear()
            st.success(f"{tab_name} guardado correctamente.")
            st.rerun()

    with c2:
        if ok:
            st.caption("Validación OK")
        else:
            st.error(msg)

    with c3:
        st.caption("Al guardar, se reescribe la pestaña completa (MVP). Para multiusuario concurrente se puede agregar control de versión.")


with tab_ped:
    editor_block("Pedidos", pedidos, key="edit_pedidos")

with tab_items:
    st.caption("Cada fila representa una pieza dentro de un pedido.")
    editor_block("Items_Pedido", items, key="edit_items")

with tab_imp:
    st.caption("Cada fila representa una impresora. Impresora_ID debe ser único.")
    editor_block("Impresoras", impresoras, key="edit_impresoras")

with tab_cal:
    st.caption("Cada fila es una asignación: Fecha + Impresora_ID + Pedido_ID.")
    editor_block("Calendario_Asignaciones", cal, key="edit_cal")

with tab_fer:
    st.caption("Factor_capacidad (opcional) debe estar entre 0 y 1. 0 = no producción, 1 = normal.")
    editor_block("Feriados", feriados, key="edit_feriados")
