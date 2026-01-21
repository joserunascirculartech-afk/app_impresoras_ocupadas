import os
import re
from datetime import date
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

# =========================
# CONFIG
# =========================
DEFAULT_SHEET_ID = "1OyCDNOH40rI2xZ2BrXCNC-PlrLzv-AV__QhKCnVxcJI"
DEFAULT_JSON_FILENAME = "app-impresoras-ocupadas-b5565c6f9b04.json"

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TABS = ["Pedidos", "Items_Pedido", "Impresoras", "Calendario_Asignaciones", "Feriados"]

# Columnas esperadas (mínimas) para no romper lógica
REQUIRED_COLS = {
    "Pedidos": ["Pedido_ID"],
    "Items_Pedido": ["Pedido_ID", "Pieza", "Tiempo_por_pieza_min", "Cantidad"],
    "Impresoras": ["Impresora_ID"],
    "Calendario_Asignaciones": ["Fecha", "Impresora_ID", "Pedido_ID"],
    "Feriados": ["Fecha"],
}

# Tipos recomendados
DATE_COLS = {
    "Pedidos": ["Fecha_ingreso", "Fecha_inicio", "Fecha_compromiso"],
    "Calendario_Asignaciones": ["Fecha"],
    "Feriados": ["Fecha"],
}
NUM_COLS = {
    "Pedidos": ["Total_min", "Total_horas", "Prioridad"],  # Prioridad puede ser num o texto; si es texto no pasa nada
    "Items_Pedido": ["Tiempo_por_pieza_min", "Cantidad", "Total_item_min", "Impresoras_asignadas", "Horas_dia"],
    "Impresoras": ["Capacidad_horas_dia"],
    "Calendario_Asignaciones": ["Horas_asignadas"],
    "Feriados": ["Factor_capacidad"],
}

st.set_page_config(page_title="CICLA 3D - Tablero", layout="wide")


# =========================
# HELPERS
# =========================
def parse_sheet_id(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    if "/" not in value and len(value) > 20:
        return value
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return m.group(1) if m else ""

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def pick_json_path(user_path: str) -> str:
    candidates = []
    if user_path:
        candidates.append(user_path.strip())
    env_path = os.getenv("CICLA_GSA_JSON", "").strip()
    if env_path:
        candidates.append(env_path)
    candidates += [DEFAULT_JSON_FILENAME, "service_account.json"]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return ""

def connect(sheet_id: str, json_path: str):
    if not sheet_id:
        st.error("Falta el ID/URL del Google Sheet.")
        st.stop()
    if not json_path or not os.path.exists(json_path):
        st.error(
            "No se encontró el JSON del Service Account.\n\n"
            f"Deja el archivo en la misma carpeta que app.py: {DEFAULT_JSON_FILENAME}\n"
            "o pega la ruta correcta en la barra lateral."
        )
        st.stop()

    creds = Credentials.from_service_account_file(json_path, scopes=SCOPE)
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
        st.error("Faltan pestañas en el Google Sheet: " + ", ".join(missing))
        st.stop()

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

def clean_df_for_save(df: pd.DataFrame) -> pd.DataFrame:
    # Evita columnas "Unnamed" que aparecen a veces
    df = df.copy()
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
    # Normaliza encabezados
    df = normalize_columns(df)
    # Quita filas completamente vacías
    df = df.dropna(how="all")
    return df

def validate_tab(tab: str, df: pd.DataFrame) -> tuple[bool, str]:
    # Columnas mínimas
    needed = REQUIRED_COLS.get(tab, [])
    missing = [c for c in needed if c not in df.columns]
    if missing:
        return False, f"Faltan columnas requeridas en '{tab}': {', '.join(missing)}"

    # Reglas específicas
    if tab == "Pedidos":
        if "Pedido_ID" in df.columns:
            ids = df["Pedido_ID"].astype(str).str.strip()
            if (ids == "").any():
                return False, "En 'Pedidos', hay filas con Pedido_ID vacío."
            if ids.duplicated().any():
                dups = ids[ids.duplicated()].unique().tolist()
                return False, f"En 'Pedidos', hay Pedido_ID duplicados: {dups}"

    if tab == "Impresoras":
        if "Impresora_ID" in df.columns:
            ids = df["Impresora_ID"].astype(str).str.strip()
            if (ids == "").any():
                return False, "En 'Impresoras', hay filas con Impresora_ID vacío."
            if ids.duplicated().any():
                dups = ids[ids.duplicated()].unique().tolist()
                return False, f"En 'Impresoras', hay Impresora_ID duplicados: {dups}"

    if tab == "Items_Pedido":
        # Debe haber Pedido_ID no vacío
        if "Pedido_ID" in df.columns:
            if (df["Pedido_ID"].astype(str).str.strip() == "").any():
                return False, "En 'Items_Pedido', hay filas con Pedido_ID vacío."
        # Si hay tiempos/cantidad, no deben ser negativos
        for c in ["Tiempo_por_pieza_min", "Cantidad"]:
            if c in df.columns:
                bad = pd.to_numeric(df[c], errors="coerce")
                if (bad < 0).any():
                    return False, f"En 'Items_Pedido', '{c}' tiene valores negativos."

    if tab == "Calendario_Asignaciones":
        # Fecha no vacía
        if "Fecha" in df.columns:
            fechas = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
            if fechas.isna().any():
                return False, "En 'Calendario_Asignaciones', hay filas con Fecha inválida/vacía."
        # Impresora_ID y Pedido_ID no vacíos
        for c in ["Impresora_ID", "Pedido_ID"]:
            if c in df.columns:
                if (df[c].astype(str).str.strip() == "").any():
                    return False, f"En 'Calendario_Asignaciones', hay filas con {c} vacío."

    if tab == "Feriados":
        if "Fecha" in df.columns:
            fechas = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
            if fechas.isna().any():
                return False, "En 'Feriados', hay filas con Fecha inválida/vacía."
        if "Factor_capacidad" in df.columns:
            fc = pd.to_numeric(df["Factor_capacidad"], errors="coerce")
            # Permite NaN, pero si existe debe estar 0..1
            bad = fc.dropna()
            if ((bad < 0) | (bad > 1)).any():
                return False, "En 'Feriados', Factor_capacidad debe estar entre 0 y 1."

    return True, "OK"

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
    df = clean_df_for_save(df)
    return df

def sheet_save_tab(sh, tab: str, df: pd.DataFrame):
    ws = sh.worksheet(tab)
    df = clean_df_for_save(df)

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
        df = sheet_load_tab(sh, tab)

        # Coerciones por tab
        if tab in DATE_COLS:
            df = coerce_dates(df, DATE_COLS[tab])
        if tab in NUM_COLS:
            df = coerce_nums(df, NUM_COLS[tab])

        out[tab] = df
    return out


# =========================
# COMPUTES (KPIs + vistas)
# =========================
def compute_totals(pedidos: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    # Calcula Total_item_min y total por pedido (si se puede)
    p = pedidos.copy()
    it = items.copy()

    if not it.empty:
        if "Total_item_min" not in it.columns:
            if "Tiempo_por_pieza_min" in it.columns and "Cantidad" in it.columns:
                it["Total_item_min"] = pd.to_numeric(it["Tiempo_por_pieza_min"], errors="coerce") * pd.to_numeric(it["Cantidad"], errors="coerce")

    if not it.empty and "Pedido_ID" in it.columns and "Total_item_min" in it.columns and "Pedido_ID" in p.columns:
        agg = it.groupby("Pedido_ID", dropna=False)["Total_item_min"].sum().reset_index()
        p = p.merge(agg, on="Pedido_ID", how="left")
        p["Total_min"] = pd.to_numeric(p.get("Total_min"), errors="coerce")
        # Si Total_min está vacío, usa el calculado
        p["Total_min"] = p["Total_min"].fillna(p["Total_item_min"])
        p["Total_horas"] = p["Total_min"] / 60

    return p

def build_calendar_pivot(cal: pd.DataFrame) -> pd.DataFrame:
    if cal.empty:
        return pd.DataFrame()
    df = cal.copy()
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
    if "Impresora_ID" in df.columns:
        df["Impresora_ID"] = df["Impresora_ID"].astype(str)
    if "Pedido_ID" not in df.columns:
        return pd.DataFrame()
    pivot = df.pivot_table(index="Fecha", columns="Impresora_ID", values="Pedido_ID", aggfunc="first")
    return pivot.sort_index()


# =========================
# UI
# =========================
st.title("CICLA 3D - Tablero de Producción")

with st.sidebar:
    st.header("Conexión")

    sheet_in = st.text_input("Google Sheet (URL o ID)", value=DEFAULT_SHEET_ID)
    sheet_id = parse_sheet_id(sheet_in) or DEFAULT_SHEET_ID

    json_in = st.text_input("JSON Service Account (ruta/archivo)", value=DEFAULT_JSON_FILENAME)
    json_path = pick_json_path(json_in)

    st.caption("El JSON debe existir en tu máquina. El Sheet debe estar compartido con el Service Account como Editor.")

    if st.button("Recargar (forzar)"):
        load_all.clear()

tables = load_all(sheet_id, json_path)

# Cálculos para vista
pedidos_calc = compute_totals(tables["Pedidos"], tables["Items_Pedido"])
items = tables["Items_Pedido"]
impresoras = tables["Impresoras"]
cal = tables["Calendario_Asignaciones"]
feriados = tables["Feriados"]

tab_resumen, tab_ped, tab_items, tab_imp, tab_cal, tab_fer = st.tabs(
    ["Resumen", "Editar Pedidos", "Editar Items", "Editar Impresoras", "Editar Calendario", "Editar Feriados"]
)

# =========================
# RESUMEN
# =========================
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
    pivot = build_calendar_pivot(cal)
    st.dataframe(pivot, use_container_width=True, height=520)

    st.subheader("Pedidos (con totales calculados)")
    st.dataframe(dfp, use_container_width=True)

# =========================
# EDITOR GENÉRICO
# =========================
def editor_block(tab_name: str, df: pd.DataFrame, key: str):
    st.subheader(tab_name)

    # Data editor
    edited = st.data_editor(
        df,
        key=key,
        use_container_width=True,
        num_rows="dynamic",
    )

    # Coerciones para evitar guardar basura
    if tab_name in DATE_COLS:
        edited = coerce_dates(edited, DATE_COLS[tab_name])
    if tab_name in NUM_COLS:
        edited = coerce_nums(edited, NUM_COLS[tab_name])

    ok, msg = validate_tab(tab_name, edited)

    c1, c2, c3 = st.columns([1, 2, 3])

    with c1:
        can_save = ok
        if st.button(f"Guardar {tab_name}", disabled=not can_save):
            sh = connect(sheet_id, json_path)
            ensure_tabs(sh)
            sheet_save_tab(sh, tab_name, edited)
            load_all.clear()
            st.success(f"{tab_name} guardado correctamente.")
            st.rerun()

    with c2:
        if not ok:
            st.error(msg)
        else:
            st.caption("Validación OK")

    with c3:
        st.caption(
            "Consejo: evita dejar IDs vacíos. En Items, Tiempo_por_pieza_min y Cantidad deben ser numéricos."
        )

# =========================
# EDITAR PEDIDOS
# =========================
with tab_ped:
    editor_block("Pedidos", tables["Pedidos"], key="edit_pedidos")

# =========================
# EDITAR ITEMS
# =========================
with tab_items:
    st.caption("Cada fila representa una pieza dentro de un pedido.")
    editor_block("Items_Pedido", tables["Items_Pedido"], key="edit_items")

# =========================
# EDITAR IMPRESORAS
# =========================
with tab_imp:
    st.caption("Cada fila representa una impresora. Impresora_ID debe ser único.")
    editor_block("Impresoras", tables["Impresoras"], key="edit_impresoras")

# =========================
# EDITAR CALENDARIO
# =========================
with tab_cal:
    st.caption("Cada fila es una asignación: Fecha + Impresora_ID + Pedido_ID.")
    editor_block("Calendario_Asignaciones", tables["Calendario_Asignaciones"], key="edit_cal")

# =========================
# EDITAR FERIADOS
# =========================
with tab_fer:
    st.caption("Factor_capacidad (opcional) debe estar entre 0 y 1. 0 = no producción, 1 = normal.")
    editor_block("Feriados", tables["Feriados"], key="edit_feriados")