import csv
import datetime as dt
import io
import json
import math
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


GENERAL_REGS = {"E1"}
CONTROLLED_REGS = {"E2"}
EXPORT_REGS = {"B1"}


@dataclass
class FlatPlan:
    general_c_per_kwh: float
    controlled_c_per_kwh: float
    feed_in_c_per_kwh: float
    supply_d_per_day: float


@dataclass
class BatteryAssumptions:
    enabled: bool
    capture_export_pct: float
    roundtrip_eff_pct: float
    installed_cost: float


@dataclass
class TouBand:
    name: str
    c_per_kwh: float
    days: str  # all | wkday | wkend
    start_hhmm: str
    end_hhmm: str


@dataclass
class TariffConfig:
    mode: str  # flat | tou
    flat_import_c_per_kwh: float
    controlled_c_per_kwh: float
    fit_c_per_kwh: float
    supply_d_per_day: float
    tou_bands: list[TouBand]
    demand_enabled: bool
    demand_c_per_kw_day: float
    demand_days: str
    demand_start_hhmm: str
    demand_end_hhmm: str


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        if isinstance(v, str):
            cleaned = v.replace(",", "").replace("$", "").strip()
            if not cleaned:
                return float(default)
            return float(cleaned)
        return float(v)
    except Exception:
        return float(default)


def _currency(v: float) -> str:
    return f"${float(v):,.2f}"


def _currency_delta(v: float) -> str:
    sign = "+" if float(v) >= 0 else "-"
    return f"{sign}${abs(float(v)):,.2f}"


def _pct(v: float) -> str:
    return f"{float(v):.1f}%"


def _normalize_text(text: str) -> str:
    t = str(text or "")
    t = t.replace("\u2013", "-").replace("\u2014", "-").replace("\xa0", " ")
    t = t.replace("â€“", "-").replace("âˆ’", "-")
    t = t.replace("\x00", "")
    return t


def _extract_first_number(text: str, pattern: str) -> Optional[float]:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return _safe_float(m.group(1), default=float("nan"))


def _extract_last_number(text: str, pattern: str) -> Optional[float]:
    matches = list(re.finditer(pattern, text, flags=re.IGNORECASE | re.DOTALL))
    if not matches:
        return None
    return _safe_float(matches[-1].group(1), default=float("nan"))


def _extract_money_pair(text: str, pattern: str) -> tuple[Optional[float], Optional[float]]:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None, None
    return _safe_float(m.group(1), default=float("nan")), _safe_float(m.group(2), default=float("nan"))


def _extract_first_number_any(text: str, patterns: list[str]) -> Optional[float]:
    for pat in patterns:
        v = _extract_first_number(text, pat)
        if v is None:
            continue
        try:
            vf = float(v)
            if not math.isnan(vf):
                return vf
        except Exception:
            continue
    return None


@st.cache_data(show_spinner=False, ttl=300)
def extract_quote_defaults_from_pdf_bytes(pdf_bytes: bytes) -> dict:
    out = {
        "system_size_kwdc": None,
        "annual_production_kwh": None,
        "ac_system_size_kw": None,
        "system_efficiency_pct": None,
        "self_consumption_pct": None,
        "daily_supply_charge_d": None,
        "import_rate_d_per_kwh": None,
        "fit_d_per_kwh": None,
        "utility_inflation_pct": None,
        "inflation_pct": None,
        "discount_rate_pct": None,
        "lifetime_years": None,
        "quote_total_cost": None,
        "quote_cost_base_net_stc": None,
        "quote_subtotal_cost": None,
        "quote_stc_credit": None,
        "quote_stc_count": None,
        "quoted_monthly_bill_before": None,
        "quoted_monthly_bill_after": None,
        "quoted_annual_bill_before": None,
        "quoted_annual_bill_after": None,
        "quoted_annual_savings": None,
        "quoted_npv": None,
        "quoted_roi_pct": None,
        "quoted_irr_pct": None,
        "quoted_discounted_payback_text": None,
        "quoted_quarterly_bill_assumption": None,
        "pv_degradation_first_year_pct": 99.0,
        "pv_degradation_annual_pct": 0.4,
    }
    if PdfReader is None or not pdf_bytes:
        return out

    pages_text = []
    reader = PdfReader(io.BytesIO(pdf_bytes))
    for p in reader.pages:
        pages_text.append(p.extract_text() or "")
    text = _normalize_text("\n".join(pages_text))

    out["system_size_kwdc"] = _extract_first_number(text, r"System size.*?([0-9]+(?:\.[0-9]+)?)\s*kWDC")
    out["annual_production_kwh"] = _extract_first_number(text, r"Estimated annual production.*?([0-9,]+)\s*kWh")
    out["ac_system_size_kw"] = _extract_first_number(text, r"AC system size.*?([0-9]+(?:\.[0-9]+)?)\s*kW")
    out["system_efficiency_pct"] = _extract_first_number(text, r"System ef\w*iency.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["self_consumption_pct"] = _extract_first_number(text, r"Self-consumption rate.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["daily_supply_charge_d"] = _extract_first_number(text, r"Daily supply charge.*?\$([0-9,]+(?:\.[0-9]+)?)")
    out["import_rate_d_per_kwh"] = _extract_first_number(text, r"Current electricity price.*?\$([0-9,]+(?:\.[0-9]+)?)")
    out["fit_d_per_kwh"] = _extract_first_number(text, r"Feed-in Tariff.*?\$([0-9,]+(?:\.[0-9]+)?)")
    out["utility_inflation_pct"] = _extract_first_number(text, r"Utility rate in\w*ation.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["inflation_pct"] = _extract_first_number(text, r"In\w*ation rate8.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["discount_rate_pct"] = _extract_first_number(text, r"Effective interest rate8.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["lifetime_years"] = _extract_first_number(text, r"System lifetime.*?([0-9]+)\s*year")
    out["quote_subtotal_cost"] = _extract_first_number(
        text, r"\bSubtotal\s*incl\.?\s*GST\s*\$([0-9,]+(?:\.[0-9]+)?)"
    )
    # Use the last explicit "Total incl GST" to avoid accidentally matching "Subtotal incl GST".
    out["quote_total_cost"] = _extract_last_number(
        text, r"\bTotal\s*incl\.?\s*GST\s*\$([0-9,]+(?:\.[0-9]+)?)"
    )
    out["quote_cost_base_net_stc"] = _extract_first_number_any(
        text,
        [
            r"Total\s*incl\.?\s*GST\s*for\s*Cost\s*Base\.?\s*Net\s*of\s*STC'?s?.*?\$([0-9,]+(?:\.[0-9]+)?)",
            r"Cost\s*Base.*?Net\s*of\s*STC'?s?.*?Total\s*incl\.?\s*GST.*?\$([0-9,]+(?:\.[0-9]+)?)",
            r"Net\s*of\s*STC'?s?.*?Total\s*incl\.?\s*GST.*?\$([0-9,]+(?:\.[0-9]+)?)",
            r"Net\s*total\s*installed\s*cost.*?\$([0-9,]+(?:\.[0-9]+)?)",
            r"Total\s*installed\s*cost.*?Net.*?STC'?s?.*?\$([0-9,]+(?:\.[0-9]+)?)",
        ],
    )
    out["quote_stc_credit"] = _extract_first_number_any(
        text,
        [
            r"[0-9]+\s*STC'?s?.{0,80}?[-\u2212]\s*\$([0-9,]+(?:\.[0-9]+)?)",
            r"STC'?s?.{0,80}?credit.{0,80}?\$([0-9,]+(?:\.[0-9]+)?)",
        ],
    )
    if out["quote_cost_base_net_stc"] is not None:
        out["quote_total_cost"] = out["quote_cost_base_net_stc"]
    elif (
        out["quote_total_cost"] is not None
        and out["quote_subtotal_cost"] is not None
        and out["quote_stc_credit"] is not None
        and abs(float(out["quote_total_cost"]) - float(out["quote_subtotal_cost"])) < 0.01
    ):
        # If total equals subtotal, treat subtotal as gross and derive net total from STC credit.
        out["quote_total_cost"] = max(float(out["quote_subtotal_cost"]) - float(out["quote_stc_credit"]), 0.0)
    out["quote_stc_count"] = _extract_first_number(text, r"([0-9]+)\s*STCs")
    out["quoted_annual_savings"] = _extract_first_number(text, r"Estimated annual savings\s*\$([0-9,]+(?:\.[0-9]+)?)")
    out["quoted_npv"] = _extract_first_number(text, r"Net present value of investment.*?\$([0-9,]+(?:\.[0-9]+)?)")
    out["quoted_roi_pct"] = _extract_first_number(text, r"Total return on investment.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["quoted_irr_pct"] = _extract_first_number(text, r"Rate of return on cash invested.*?([0-9]+(?:\.[0-9]+)?)\s*%")
    out["quoted_quarterly_bill_assumption"] = _extract_first_number(
        text, r"Quarterly electricity bills.*?\$([0-9,]+(?:\.[0-9]+)?)"
    )
    out["pv_degradation_first_year_pct"] = _extract_first_number(
        text, r"([0-9]+(?:\.[0-9]+)?)\s*%\s*for the\s*rst year"
    ) or out["pv_degradation_first_year_pct"]
    out["pv_degradation_annual_pct"] = abs(
        _extract_first_number(text, r"-\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*per year to year")
        or out["pv_degradation_annual_pct"]
    )

    m_payback = re.search(
        r"Discounted payback period.*?([0-9]+(?:\s*-\s*[0-9]+)?)\s*years",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_payback:
        out["quoted_discounted_payback_text"] = m_payback.group(1).replace(" ", "")

    m1, m2 = _extract_money_pair(
        text, r"Average monthly bill\s*\$([0-9,]+\.[0-9]{2})\s*\$([0-9,]+\.[0-9]{2})"
    )
    out["quoted_monthly_bill_before"] = m1
    out["quoted_monthly_bill_after"] = m2

    a1, a2 = _extract_money_pair(
        text, r"Annual bill\s*\$([0-9,]+\.[0-9]{2})\s*\$([0-9,]+\.[0-9]{2})"
    )
    out["quoted_annual_bill_before"] = a1
    out["quoted_annual_bill_after"] = a2
    return out


def _ocr_pdf_text_if_available(pdf_bytes: bytes) -> tuple[str, Optional[str]]:
    """Best-effort OCR for image-only invoices when OCR deps are installed."""
    try:
        import pytesseract  # type: ignore
        from pdf2image import convert_from_bytes  # type: ignore
    except Exception:
        return "", "OCR dependencies not installed (`pytesseract` + `pdf2image`)."

    try:
        images = convert_from_bytes(pdf_bytes, dpi=250)
    except Exception as ex:
        return "", f"OCR PDF-to-image conversion failed: {ex}"

    chunks: list[str] = []
    try:
        for img in images:
            txt = pytesseract.image_to_string(img) or ""
            if txt:
                chunks.append(txt)
    except Exception as ex:
        return "", f"OCR text extraction failed: {ex}"

    return "\n".join(chunks), None


@st.cache_data(show_spinner=False, ttl=300)
def extract_invoice_tariff_defaults_from_pdf_bytes(pdf_bytes: bytes) -> dict:
    out = {
        "invoice_import_rate_c_per_kwh": None,
        "invoice_controlled_rate_c_per_kwh": None,
        "invoice_fit_rate_c_per_kwh": None,
        "invoice_supply_d_per_day": None,
        "invoice_pages": 0,
        "invoice_text_chars": 0,
        "invoice_text_extract_ok": False,
        "invoice_ocr_used": False,
        "invoice_extract_note": "",
    }
    if PdfReader is None or not pdf_bytes:
        return out

    pages_text = []
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out["invoice_pages"] = int(len(reader.pages))
    for p in reader.pages:
        pages_text.append(p.extract_text() or "")
    text = _normalize_text("\n".join(pages_text))
    text_chars = len(text.strip())
    out["invoice_text_chars"] = int(text_chars)

    # Image-only PDFs often have no text layer; try OCR if available.
    if text_chars < 20:
        ocr_text, ocr_note = _ocr_pdf_text_if_available(pdf_bytes)
        if ocr_text and len(ocr_text.strip()) >= 20:
            text = _normalize_text(ocr_text)
            out["invoice_ocr_used"] = True
            out["invoice_text_chars"] = int(len(text.strip()))
            out["invoice_extract_note"] = "OCR fallback used."
        else:
            note = "No text layer detected in invoice PDF."
            if ocr_note:
                note = f"{note} {ocr_note}"
            out["invoice_extract_note"] = note
    else:
        out["invoice_extract_note"] = "Parsed from text layer."

    out["invoice_text_extract_ok"] = bool(len(str(text).strip()) >= 20)

    out["invoice_import_rate_c_per_kwh"] = _extract_first_number_any(
        text,
        [
            r"(?:General\s*usage|Anytime|Usage\s*charge|Consumption)[^$c\n]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*c\s*(?:/|per\s*)kwh",
            r"(?:Peak|Shoulder|Off-peak)\s*usage[^$c\n]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*c\s*(?:/|per\s*)kwh",
        ],
    )
    if out["invoice_import_rate_c_per_kwh"] is None:
        import_rate_d = _extract_first_number_any(
            text,
            [
                r"(?:General\s*usage|Anytime|Usage\s*charge|Consumption)[^\n]{0,120}?\$([0-9]+(?:\.[0-9]+)?)\s*(?:/|per\s*)kwh",
            ],
        )
        if import_rate_d is not None:
            out["invoice_import_rate_c_per_kwh"] = float(import_rate_d) * 100.0

    out["invoice_controlled_rate_c_per_kwh"] = _extract_first_number_any(
        text,
        [
            r"(?:Controlled\s*load|CL\s*[0-9]|Off-peak\s*controlled)[^$c\n]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*c\s*(?:/|per\s*)kwh",
        ],
    )
    if out["invoice_controlled_rate_c_per_kwh"] is None:
        controlled_rate_d = _extract_first_number_any(
            text,
            [
                r"(?:Controlled\s*load|CL\s*[0-9]|Off-peak\s*controlled)[^\n]{0,120}?\$([0-9]+(?:\.[0-9]+)?)\s*(?:/|per\s*)kwh",
            ],
        )
        if controlled_rate_d is not None:
            out["invoice_controlled_rate_c_per_kwh"] = float(controlled_rate_d) * 100.0

    out["invoice_fit_rate_c_per_kwh"] = _extract_first_number_any(
        text,
        [
            r"(?:Feed[-\s]*in(?:\s*tariff)?|Solar\s*feed)[^$c\n]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*c\s*(?:/|per\s*)kwh",
        ],
    )
    if out["invoice_fit_rate_c_per_kwh"] is None:
        fit_rate_d = _extract_first_number_any(
            text,
            [
                r"(?:Feed[-\s]*in(?:\s*tariff)?|Solar\s*feed)[^\n]{0,120}?\$([0-9]+(?:\.[0-9]+)?)\s*(?:/|per\s*)kwh",
            ],
        )
        if fit_rate_d is not None:
            out["invoice_fit_rate_c_per_kwh"] = float(fit_rate_d) * 100.0

    out["invoice_supply_d_per_day"] = _extract_first_number_any(
        text,
        [
            r"(?:Daily\s*supply\s*charge|Supply\s*charge|Service\s*to\s*property|Daily\s*charge)[^\n]{0,120}?\$([0-9]+(?:\.[0-9]+)?)\s*(?:/|per\s*)day",
        ],
    )
    if out["invoice_supply_d_per_day"] is None:
        supply_c_per_day = _extract_first_number_any(
            text,
            [
                r"(?:Daily\s*supply\s*charge|Supply\s*charge|Service\s*to\s*property|Daily\s*charge)[^$c\n]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*c\s*(?:/|per\s*)day",
            ],
        )
        if supply_c_per_day is not None:
            out["invoice_supply_d_per_day"] = float(supply_c_per_day) / 100.0

    return out


@st.cache_data(show_spinner=False, ttl=300)
def read_nem12_5min_bytes(file_bytes: bytes) -> pd.DataFrame:
    if not file_bytes:
        return pd.DataFrame(columns=["register", "timestamp", "kwh"])

    rows = []
    current_register = None
    content = file_bytes.decode("utf-8", errors="replace").splitlines()
    reader = csv.reader(content)
    for rec in reader:
        if not rec:
            continue
        rec_type = rec[0].strip()
        if rec_type == "200":
            current_register = rec[3].strip() if len(rec) > 3 else None
            continue
        if rec_type != "300" or not current_register:
            continue
        if len(rec) < 3:
            continue
        try:
            day = dt.datetime.strptime(rec[1], "%Y%m%d").date()
        except Exception:
            continue
        vals = rec[2 : 2 + 288]
        base = dt.datetime.combine(day, dt.time(0, 0))
        for i, v in enumerate(vals):
            if v is None or v == "":
                continue
            try:
                kwh = float(v)
            except Exception:
                kwh = 0.0
            ts = base + dt.timedelta(minutes=5 * i)
            rows.append((current_register, ts, kwh))

    df = pd.DataFrame(rows, columns=["register", "timestamp", "kwh"])
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["register"] = df["register"].astype(str).str.strip()
    df["kwh"] = pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(show_spinner=False, ttl=300)
def read_solar_profile_bytes(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    if not file_bytes:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    name = str(file_name or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        raw = pd.read_excel(io.BytesIO(file_bytes))
    else:
        raw = pd.read_csv(io.StringIO(file_bytes.decode("utf-8-sig", errors="replace")))

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    cols_lower = {c: str(c).strip().lower() for c in df.columns}

    dt_col = None
    for c, lc in cols_lower.items():
        if "date" in lc and "time" in lc:
            dt_col = c
            break
    if dt_col is None:
        for c, lc in cols_lower.items():
            if "timestamp" in lc or "datetime" in lc:
                dt_col = c
                break
    if dt_col is None and len(df.columns) >= 1:
        dt_col = df.columns[0]

    pv_col = None
    for c, lc in cols_lower.items():
        if ("pv" in lc and ("prod" in lc or "gen" in lc)) or ("solar" in lc and "prod" in lc):
            pv_col = c
            break
    if pv_col is None:
        for c, lc in cols_lower.items():
            if "production" in lc or "energy" in lc:
                pv_col = c
                break
    if pv_col is None:
        pv_candidates = [c for c in df.columns if c != dt_col]
        pv_col = pv_candidates[0] if pv_candidates else None
    if dt_col is None or pv_col is None:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    out = df[[dt_col, pv_col]].copy()
    out.columns = ["timestamp_raw", "pv_raw"]
    ts = pd.to_datetime(out["timestamp_raw"], format="%d.%m.%Y %H:%M", errors="coerce")
    if ts.isna().any():
        ts2 = pd.to_datetime(out.loc[ts.isna(), "timestamp_raw"], dayfirst=True, errors="coerce")
        ts.loc[ts.isna()] = ts2
    out["timestamp"] = ts
    out["pv_num"] = pd.to_numeric(out["pv_raw"], errors="coerce")
    out = out.dropna(subset=["timestamp", "pv_num"])
    if out.empty:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    pv_col_name = str(pv_col).lower()
    if "kwh" in pv_col_name:
        out["pv_kwh"] = out["pv_num"].astype(float)
    elif "wh" in pv_col_name:
        out["pv_kwh"] = out["pv_num"].astype(float) / 1000.0
    else:
        q99 = float(out["pv_num"].quantile(0.99))
        out["pv_kwh"] = out["pv_num"].astype(float) / 1000.0 if q99 > 20.0 else out["pv_num"].astype(float)

    out["pv_kwh"] = pd.to_numeric(out["pv_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["pv_kwh"].sum().sort_values("timestamp")
    return out.reset_index(drop=True)


def align_solar_to_intervals(df_solar: pd.DataFrame, df_int: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    if (
        df_solar is None
        or df_solar.empty
        or df_int is None
        or df_int.empty
        or "timestamp" not in df_int.columns
        or "timestamp" not in df_solar.columns
        or "pv_kwh" not in df_solar.columns
    ):
        return pd.DataFrame(columns=["timestamp", "pv_kwh"]), 0.0

    base_ts = pd.DataFrame({"timestamp": pd.to_datetime(df_int["timestamp"], errors="coerce").dropna().drop_duplicates()})
    base_ts = base_ts.sort_values("timestamp").reset_index(drop=True)
    s = df_solar.copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], errors="coerce")
    s["pv_kwh"] = pd.to_numeric(s["pv_kwh"], errors="coerce").fillna(0.0).astype(float)
    s = s.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["pv_kwh"].sum()
    m = base_ts.merge(s, on="timestamp", how="left")
    matched = float(m["pv_kwh"].notna().mean() * 100.0) if len(m) > 0 else 0.0
    m["pv_kwh"] = m["pv_kwh"].fillna(0.0).clip(lower=0.0)
    return m, matched


def _interval_minutes(ts: pd.Series) -> float:
    s = pd.to_datetime(ts, errors="coerce").dropna().drop_duplicates().sort_values()
    if len(s) < 2:
        return 5.0
    mins = float((s.diff().dropna().dt.total_seconds() / 60.0).median())
    return mins if mins > 0 else 5.0


def _intervals_wide_from_long(df_int: pd.DataFrame) -> pd.DataFrame:
    if df_int is None or df_int.empty:
        return pd.DataFrame(columns=["timestamp", "general_kwh", "controlled_kwh", "export_kwh"])
    d = df_int.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d["kwh"] = pd.to_numeric(d["kwh"], errors="coerce").fillna(0.0)
    d = d.dropna(subset=["timestamp"])
    if d.empty:
        return pd.DataFrame(columns=["timestamp", "general_kwh", "controlled_kwh", "export_kwh"])
    d["register"] = d["register"].astype(str).str.strip().str.upper()

    g = d[d["register"].isin(GENERAL_REGS)].groupby("timestamp", as_index=False)["kwh"].sum().rename(columns={"kwh": "general_kwh"})
    c = d[d["register"].isin(CONTROLLED_REGS)].groupby("timestamp", as_index=False)["kwh"].sum().rename(columns={"kwh": "controlled_kwh"})
    e = d[d["register"].isin(EXPORT_REGS)].groupby("timestamp", as_index=False)["kwh"].sum().rename(columns={"kwh": "export_kwh"})

    base = pd.DataFrame({"timestamp": d["timestamp"].drop_duplicates().sort_values()})
    out = base.merge(g, on="timestamp", how="left").merge(c, on="timestamp", how="left").merge(e, on="timestamp", how="left")
    for col in ["general_kwh", "controlled_kwh", "export_kwh"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).clip(lower=0.0)
    return out.sort_values("timestamp").reset_index(drop=True)


def _intervals_long_from_wide(wide: pd.DataFrame) -> pd.DataFrame:
    if wide is None or wide.empty:
        return pd.DataFrame(columns=["register", "timestamp", "kwh"])
    rows = []
    for _, r in wide.iterrows():
        ts = pd.to_datetime(r.get("timestamp"), errors="coerce")
        if pd.isna(ts):
            continue
        rows.append(("E1", ts, float(max(_safe_float(r.get("general_kwh", 0.0)), 0.0))))
        rows.append(("E2", ts, float(max(_safe_float(r.get("controlled_kwh", 0.0)), 0.0))))
        rows.append(("B1", ts, float(max(_safe_float(r.get("export_kwh", 0.0)), 0.0))))
    out = pd.DataFrame(rows, columns=["register", "timestamp", "kwh"])
    return out.sort_values(["timestamp", "register"]).reset_index(drop=True)


def _day_match(ts: pd.Timestamp, mode: str) -> bool:
    if mode == "all":
        return True
    dow = int(ts.dayofweek)
    if mode == "wkday":
        return dow < 5
    if mode == "wkend":
        return dow >= 5
    return True


def _minutes_of_day(ts: pd.Timestamp) -> int:
    return int(ts.hour) * 60 + int(ts.minute)


def _parse_hhmm_minutes(hhmm: str) -> int:
    try:
        hh, mm = [int(x) for x in str(hhmm).split(":")]
        hh = max(0, min(23, hh))
        mm = max(0, min(59, mm))
        return hh * 60 + mm
    except Exception:
        return 0


def _in_window(ts: pd.Timestamp, start_hhmm: str, end_hhmm: str) -> bool:
    m = _minutes_of_day(ts)
    st = _parse_hhmm_minutes(start_hhmm)
    en = _parse_hhmm_minutes(end_hhmm)
    if st == en:
        return True
    if st < en:
        return st <= m < en
    return (m >= st) or (m < en)


def _tou_rate_for_ts(ts: pd.Timestamp, bands: list[TouBand], fallback_c: float) -> float:
    for b in bands:
        if _day_match(ts, b.days) and _in_window(ts, b.start_hhmm, b.end_hhmm):
            return float(b.c_per_kwh)
    return float(fallback_c)


def _effective_tou_import_rate_c_per_kwh(bands: list[TouBand], fallback_c: float, step_minutes: int = 5) -> float:
    """Weekly time-weighted effective import rate for TOU bands."""
    if not bands:
        return float(fallback_c)
    step = max(int(step_minutes), 1)
    base = pd.Timestamp("2024-01-01 00:00:00")  # Monday
    total = 0.0
    count = 0
    for d in range(7):
        day_base = base + pd.Timedelta(days=d)
        for m in range(0, 24 * 60, step):
            ts = day_base + pd.Timedelta(minutes=m)
            total += float(_tou_rate_for_ts(ts, bands, float(fallback_c)))
            count += 1
    if count <= 0:
        return float(fallback_c)
    return float(total / float(count))


def build_profile_from_annual_target(df_int: pd.DataFrame, annual_kwh: float) -> pd.DataFrame:
    if df_int is None or df_int.empty or annual_kwh <= 0:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])
    ts = pd.to_datetime(df_int["timestamp"], errors="coerce").dropna().drop_duplicates().sort_values()
    if ts.empty:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    prof = pd.DataFrame({"timestamp": ts})
    prof["hour"] = prof["timestamp"].dt.hour + (prof["timestamp"].dt.minute / 60.0)
    # Daylight proxy for Brisbane-ish profile
    prof["shape"] = prof["hour"].apply(
        lambda h: (math.sin(math.pi * ((h - 6.0) / 12.0)) ** 1.5) if 6.0 < h < 18.0 else 0.0
    )
    prof["shape"] = pd.to_numeric(prof["shape"], errors="coerce").fillna(0.0).clip(lower=0.0)
    if float(prof["shape"].sum()) <= 0:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])

    dataset_days = float(prof["timestamp"].dt.date.nunique())
    period_target_kwh = float(annual_kwh) * (dataset_days / 365.0) if dataset_days > 0 else 0.0
    prof["pv_kwh"] = prof["shape"] / float(prof["shape"].sum()) * float(period_target_kwh)
    return prof[["timestamp", "pv_kwh"]].copy()


def rescale_profile_to_annual_target(solar_aligned: pd.DataFrame, annual_kwh: float, df_int: pd.DataFrame) -> pd.DataFrame:
    if (
        solar_aligned is None
        or solar_aligned.empty
        or df_int is None
        or df_int.empty
        or annual_kwh <= 0
    ):
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])
    s = solar_aligned.copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], errors="coerce")
    s["pv_kwh"] = pd.to_numeric(s["pv_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)
    s = s.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["pv_kwh"].sum()
    total = float(s["pv_kwh"].sum())
    if total <= 0:
        return pd.DataFrame(columns=["timestamp", "pv_kwh"])
    dataset_days = float(pd.to_datetime(df_int["timestamp"], errors="coerce").dropna().dt.date.nunique())
    period_target_kwh = float(annual_kwh) * (dataset_days / 365.0) if dataset_days > 0 else 0.0
    scale = period_target_kwh / total if total > 0 else 0.0
    s["pv_kwh"] = s["pv_kwh"] * float(scale)
    return s


def apply_pv_to_wide(
    wide_base: pd.DataFrame,
    pv_profile: pd.DataFrame,
    assume_no_existing_export: bool,
    add_existing_export_to_general: bool,
) -> tuple[pd.DataFrame, dict]:
    meta = {
        "pv_generated_kwh": 0.0,
        "self_consumed_kwh": 0.0,
        "exported_pv_kwh": 0.0,
        "solar_coverage_pct": 0.0,
    }
    if wide_base is None or wide_base.empty:
        return pd.DataFrame(columns=["timestamp", "general_kwh", "controlled_kwh", "export_kwh"]), meta

    w = wide_base.copy()
    for c in ["general_kwh", "controlled_kwh", "export_kwh"]:
        if c not in w.columns:
            w[c] = 0.0
        w[c] = pd.to_numeric(w[c], errors="coerce").fillna(0.0).clip(lower=0.0)

    if assume_no_existing_export:
        if add_existing_export_to_general:
            w["general_kwh"] = w["general_kwh"] + w["export_kwh"]
        w["export_kwh"] = 0.0

    p = pv_profile.copy() if isinstance(pv_profile, pd.DataFrame) else pd.DataFrame(columns=["timestamp", "pv_kwh"])
    if not p.empty:
        p["timestamp"] = pd.to_datetime(p["timestamp"], errors="coerce")
        p["pv_kwh"] = pd.to_numeric(p["pv_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)
        p = p.dropna(subset=["timestamp"]).groupby("timestamp", as_index=False)["pv_kwh"].sum()
    else:
        p = pd.DataFrame(columns=["timestamp", "pv_kwh"])

    m = w.merge(p, on="timestamp", how="left")
    m["pv_kwh"] = pd.to_numeric(m["pv_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)
    m["pv_to_general"] = m[["pv_kwh", "general_kwh"]].min(axis=1)
    m["general_after"] = (m["general_kwh"] - m["pv_to_general"]).clip(lower=0.0)
    m["controlled_after"] = m["controlled_kwh"]
    m["export_after"] = (m["export_kwh"] + (m["pv_kwh"] - m["pv_to_general"]).clip(lower=0.0)).clip(lower=0.0)

    out = pd.DataFrame(
        {
            "timestamp": m["timestamp"],
            "general_kwh": m["general_after"],
            "controlled_kwh": m["controlled_after"],
            "export_kwh": m["export_after"],
        }
    )

    pv_total = float(m["pv_kwh"].sum())
    self_cons = float(m["pv_to_general"].sum())
    import_after = float((m["general_after"] + m["controlled_after"]).sum())
    load_after = import_after + self_cons
    meta["pv_generated_kwh"] = pv_total
    meta["self_consumed_kwh"] = self_cons
    meta["exported_pv_kwh"] = max(pv_total - self_cons, 0.0)
    meta["solar_coverage_pct"] = (self_cons / load_after * 100.0) if load_after > 0 else 0.0
    return out.sort_values("timestamp").reset_index(drop=True), meta


def apply_battery_dispatch_to_wide(
    wide_in: pd.DataFrame,
    capacity_kwh: float,
    power_kw: float,
    roundtrip_eff_pct: float,
    reserve_pct: float,
    init_soc_pct: float,
    discharge_peak_only: bool,
    tou_bands: list[TouBand],
) -> tuple[pd.DataFrame, dict]:
    meta = {
        "enabled": False,
        "battery_charge_kwh": 0.0,
        "battery_discharge_kwh": 0.0,
        "import_reduction_kwh": 0.0,
        "export_reduction_kwh": 0.0,
        "end_soc_kwh": 0.0,
    }
    if wide_in is None or wide_in.empty:
        return pd.DataFrame(columns=["timestamp", "general_kwh", "controlled_kwh", "export_kwh"]), meta

    cap = max(float(capacity_kwh), 0.0)
    p_kw = max(float(power_kw), 0.0)
    if cap <= 0 or p_kw <= 0:
        return wide_in.copy(), meta

    d = wide_in.copy().sort_values("timestamp").reset_index(drop=True)
    for c in ["general_kwh", "controlled_kwh", "export_kwh"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0).clip(lower=0.0)
    d["import_kwh"] = d["general_kwh"] + d["controlled_kwh"]

    dt_minutes = _interval_minutes(d["timestamp"])
    dt_hours = dt_minutes / 60.0
    power_limit = p_kw * dt_hours
    reserve = cap * min(max(float(reserve_pct), 0.0), 100.0) / 100.0
    soc = cap * min(max(float(init_soc_pct), 0.0), 100.0) / 100.0
    soc = min(max(soc, reserve), cap)

    rt = min(max(float(roundtrip_eff_pct), 1.0), 100.0) / 100.0
    eta_c = math.sqrt(rt)
    eta_d = math.sqrt(rt)

    charge_total = 0.0
    discharge_total = 0.0
    import_before = float(d["import_kwh"].sum())
    export_before = float(d["export_kwh"].sum())
    new_rows = []

    for _, r in d.iterrows():
        ts = pd.to_datetime(r["timestamp"])
        g = float(r["general_kwh"])
        c = float(r["controlled_kwh"])
        exp = float(r["export_kwh"])
        imp = g + c

        # Charge from PV export first.
        charge_in = min(exp, power_limit, max((cap - soc) / max(eta_c, 1e-9), 0.0))
        if charge_in > 0:
            soc += charge_in * eta_c
            exp -= charge_in
            charge_total += charge_in

        allow_discharge = True
        if discharge_peak_only:
            allow_discharge = False
            for b in tou_bands:
                if str(b.name).lower().startswith("peak"):
                    if _day_match(ts, b.days) and _in_window(ts, b.start_hhmm, b.end_hhmm):
                        allow_discharge = True
                        break
        if allow_discharge:
            discharge_out = min(imp, power_limit, max((soc - reserve) * eta_d, 0.0))
            if discharge_out > 0:
                soc -= discharge_out / max(eta_d, 1e-9)
                imp -= discharge_out
                discharge_total += discharge_out

        # Preserve original controlled-share split.
        if (g + c) > 0:
            c_share = c / (g + c)
            c_new = imp * c_share
            g_new = imp - c_new
        else:
            g_new = 0.0
            c_new = 0.0

        new_rows.append((ts, max(g_new, 0.0), max(c_new, 0.0), max(exp, 0.0)))

    out = pd.DataFrame(new_rows, columns=["timestamp", "general_kwh", "controlled_kwh", "export_kwh"])
    out["import_kwh"] = out["general_kwh"] + out["controlled_kwh"]
    import_after = float(out["import_kwh"].sum())
    export_after = float(out["export_kwh"].sum())

    meta["enabled"] = True
    meta["battery_charge_kwh"] = float(charge_total)
    meta["battery_discharge_kwh"] = float(discharge_total)
    meta["import_reduction_kwh"] = max(import_before - import_after, 0.0)
    meta["export_reduction_kwh"] = max(export_before - export_after, 0.0)
    meta["end_soc_kwh"] = float(soc)
    return out[["timestamp", "general_kwh", "controlled_kwh", "export_kwh"]], meta


def simulate_bill_interval(wide: pd.DataFrame, tariff: TariffConfig) -> dict:
    out = {
        "days": 0.0,
        "general_kwh": 0.0,
        "controlled_kwh": 0.0,
        "export_kwh": 0.0,
        "usage_cost": 0.0,
        "controlled_cost": 0.0,
        "fit_credit": 0.0,
        "supply_cost": 0.0,
        "demand_cost": 0.0,
        "annual_bill": 0.0,
    }
    if wide is None or wide.empty:
        return out
    d = wide.copy().sort_values("timestamp").reset_index(drop=True)
    for c in ["general_kwh", "controlled_kwh", "export_kwh"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0).clip(lower=0.0)
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.dropna(subset=["timestamp"])
    if d.empty:
        return out

    days = float(d["timestamp"].dt.date.nunique())
    out["days"] = days
    out["general_kwh"] = float(d["general_kwh"].sum())
    out["controlled_kwh"] = float(d["controlled_kwh"].sum())
    out["export_kwh"] = float(d["export_kwh"].sum())

    if tariff.mode == "tou":
        d["general_rate_c"] = d["timestamp"].apply(lambda ts: _tou_rate_for_ts(ts, tariff.tou_bands, tariff.flat_import_c_per_kwh))
    else:
        d["general_rate_c"] = float(tariff.flat_import_c_per_kwh)

    usage_cost = float((d["general_kwh"] * d["general_rate_c"]).sum() / 100.0)
    controlled_cost = float((d["controlled_kwh"] * float(tariff.controlled_c_per_kwh)).sum() / 100.0)
    fit_credit = float((d["export_kwh"] * float(tariff.fit_c_per_kwh)).sum() / 100.0)
    supply_cost = float(tariff.supply_d_per_day) * days

    demand_cost = 0.0
    if tariff.demand_enabled:
        dt_minutes = _interval_minutes(d["timestamp"])
        win = max(int(round(30.0 / dt_minutes)), 1)
        d["imp_total"] = d["general_kwh"] + d["controlled_kwh"]
        d["demand_ok"] = d["timestamp"].apply(
            lambda ts: _day_match(ts, tariff.demand_days) and _in_window(ts, tariff.demand_start_hhmm, tariff.demand_end_hhmm)
        )
        d["imp_for_demand"] = d["imp_total"].where(d["demand_ok"], 0.0)
        monthly = d.set_index("timestamp")["imp_for_demand"].resample("MS")
        for month_start, s in monthly:
            if s.empty:
                continue
            r = s.rolling(win, min_periods=1).sum()
            max_kw = float(r.max()) * (60.0 / 30.0)
            days_in_month = int((month_start + pd.offsets.MonthEnd(1)).day)
            demand_cost += max_kw * float(tariff.demand_c_per_kw_day / 100.0) * days_in_month

    total = max(0.0, usage_cost + controlled_cost + supply_cost + demand_cost - fit_credit)
    annual_scale = (365.0 / days) if days > 0 else 0.0
    out["usage_cost"] = usage_cost
    out["controlled_cost"] = controlled_cost
    out["fit_credit"] = fit_credit
    out["supply_cost"] = supply_cost
    out["demand_cost"] = demand_cost
    out["annual_bill"] = float(total * annual_scale)
    return out

def summarize_interval_energy(df_int: pd.DataFrame) -> dict:
    out = {
        "days": 0.0,
        "general_kwh": 0.0,
        "controlled_kwh": 0.0,
        "export_kwh": 0.0,
        "import_kwh": 0.0,
        "annual_general_kwh": 0.0,
        "annual_controlled_kwh": 0.0,
        "annual_export_kwh": 0.0,
        "annual_import_kwh": 0.0,
    }
    if df_int is None or df_int.empty:
        return out
    d = df_int.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d["kwh"] = pd.to_numeric(d["kwh"], errors="coerce").fillna(0.0)
    d = d.dropna(subset=["timestamp"])
    if d.empty:
        return out
    d["register"] = d["register"].astype(str).str.strip().str.upper()
    out["days"] = float(d["timestamp"].dt.date.nunique())
    out["general_kwh"] = float(d.loc[d["register"].isin(GENERAL_REGS), "kwh"].sum())
    out["controlled_kwh"] = float(d.loc[d["register"].isin(CONTROLLED_REGS), "kwh"].sum())
    out["export_kwh"] = float(d.loc[d["register"].isin(EXPORT_REGS), "kwh"].sum())
    out["import_kwh"] = float(out["general_kwh"] + out["controlled_kwh"])
    scale = (365.0 / out["days"]) if out["days"] > 0 else 0.0
    out["annual_general_kwh"] = out["general_kwh"] * scale
    out["annual_controlled_kwh"] = out["controlled_kwh"] * scale
    out["annual_export_kwh"] = out["export_kwh"] * scale
    out["annual_import_kwh"] = out["import_kwh"] * scale
    return out


def estimate_solar_metrics(df_int: pd.DataFrame, solar_aligned: pd.DataFrame) -> dict:
    metrics = {
        "pv_generated_kwh": 0.0,
        "self_consumed_kwh": 0.0,
        "self_consumption_pct": 0.0,
        "solar_coverage_pct": 0.0,
    }
    if df_int is None or df_int.empty or solar_aligned is None or solar_aligned.empty:
        return metrics
    s = summarize_interval_energy(df_int)
    import_kwh = float(s.get("import_kwh", 0.0))
    export_kwh = float(s.get("export_kwh", 0.0))
    pv_kwh = float(pd.to_numeric(solar_aligned["pv_kwh"], errors="coerce").fillna(0.0).sum())
    self_cons_kwh = max(pv_kwh - export_kwh, 0.0)
    total_load = import_kwh + self_cons_kwh
    self_cons_pct = (self_cons_kwh / pv_kwh * 100.0) if pv_kwh > 0 else 0.0
    coverage_pct = (self_cons_kwh / total_load * 100.0) if total_load > 0 else 0.0
    metrics.update(
        {
            "pv_generated_kwh": pv_kwh,
            "self_consumed_kwh": self_cons_kwh,
            "self_consumption_pct": self_cons_pct,
            "solar_coverage_pct": coverage_pct,
        }
    )
    return metrics


def annual_bill_from_energy(general_kwh: float, controlled_kwh: float, export_kwh: float, plan: FlatPlan) -> float:
    usage_cost = (general_kwh * plan.general_c_per_kwh + controlled_kwh * plan.controlled_c_per_kwh) / 100.0
    fit_credit = (export_kwh * plan.feed_in_c_per_kwh) / 100.0
    supply_cost = plan.supply_d_per_day * 365.0
    return max(0.0, usage_cost + supply_cost - fit_credit)


def derive_usage_from_quarterly_bill(quarterly_bill: float, plan: FlatPlan) -> tuple[float, float, float]:
    annual_bill = float(max(quarterly_bill, 0.0)) * 4.0
    annual_supply = float(plan.supply_d_per_day) * 365.0
    usage_component = max(annual_bill - annual_supply, 0.0)
    rate = max(float(plan.general_c_per_kwh), 1e-9) / 100.0
    general_kwh = usage_component / rate
    return general_kwh, 0.0, 0.0


def apply_solar_quote_model(
    base_general_kwh: float,
    base_controlled_kwh: float,
    base_export_kwh: float,
    annual_production_kwh: float,
    self_consumption_pct: float,
) -> dict:
    annual_production_kwh = max(float(annual_production_kwh), 0.0)
    self_consumption_pct = min(max(float(self_consumption_pct), 0.0), 100.0)

    import_before = max(float(base_general_kwh), 0.0) + max(float(base_controlled_kwh), 0.0)
    controlled_share = (max(float(base_controlled_kwh), 0.0) / import_before) if import_before > 0 else 0.0

    self_consumed_kwh = annual_production_kwh * (self_consumption_pct / 100.0)
    exported_solar_kwh = max(annual_production_kwh - self_consumed_kwh, 0.0)
    import_after = max(import_before - self_consumed_kwh, 0.0)
    controlled_after = import_after * controlled_share
    general_after = import_after - controlled_after
    export_after = max(float(base_export_kwh), 0.0) + exported_solar_kwh

    solar_coverage_pct = (self_consumed_kwh / (import_after + self_consumed_kwh) * 100.0) if (import_after + self_consumed_kwh) > 0 else 0.0
    return {
        "general_kwh": general_after,
        "controlled_kwh": controlled_after,
        "export_kwh": export_after,
        "self_consumed_kwh": self_consumed_kwh,
        "exported_solar_kwh": exported_solar_kwh,
        "solar_coverage_pct": solar_coverage_pct,
    }


def apply_battery_overlay(
    general_kwh: float,
    controlled_kwh: float,
    export_kwh: float,
    batt: BatteryAssumptions,
) -> dict:
    import_before = max(float(general_kwh), 0.0) + max(float(controlled_kwh), 0.0)
    controlled_share = (max(float(controlled_kwh), 0.0) / import_before) if import_before > 0 else 0.0
    if not batt.enabled:
        return {
            "general_kwh": max(float(general_kwh), 0.0),
            "controlled_kwh": max(float(controlled_kwh), 0.0),
            "export_kwh": max(float(export_kwh), 0.0),
            "battery_delivered_kwh": 0.0,
            "battery_captured_export_kwh": 0.0,
        }

    capture_frac = min(max(float(batt.capture_export_pct), 0.0), 100.0) / 100.0
    eff_frac = min(max(float(batt.roundtrip_eff_pct), 0.0), 100.0) / 100.0
    captured_export_kwh = max(float(export_kwh), 0.0) * capture_frac
    delivered_kwh = min(captured_export_kwh * eff_frac, import_before)

    import_after = max(import_before - delivered_kwh, 0.0)
    controlled_after = import_after * controlled_share
    general_after = import_after - controlled_after
    export_after = max(float(export_kwh), 0.0) - captured_export_kwh

    return {
        "general_kwh": general_after,
        "controlled_kwh": controlled_after,
        "export_kwh": max(export_after, 0.0),
        "battery_delivered_kwh": delivered_kwh,
        "battery_captured_export_kwh": captured_export_kwh,
    }


def build_annual_savings_series(
    year1_savings: float,
    years: int,
    utility_inflation_pct: float,
    first_year_perf_pct: float,
    annual_degradation_pct: float,
) -> list[float]:
    y1 = float(year1_savings)
    n = max(int(years), 1)
    infl = float(utility_inflation_pct) / 100.0
    fyp = min(max(float(first_year_perf_pct) / 100.0, 0.0), 1.5)
    deg = min(max(float(annual_degradation_pct) / 100.0, 0.0), 0.2)
    series = []
    for yr in range(1, n + 1):
        perf_factor = fyp * ((1.0 - deg) ** max(0, yr - 1))
        tariff_factor = (1.0 + infl) ** max(0, yr - 1)
        series.append(y1 * perf_factor * tariff_factor)
    return series


def npv(cashflows: list[float], discount_rate_pct: float) -> float:
    r = float(discount_rate_pct) / 100.0
    total = 0.0
    for i, cf in enumerate(cashflows):
        if i == 0:
            total += float(cf)
        else:
            total += float(cf) / ((1.0 + r) ** i)
    return total


def irr_bisection(cashflows: list[float], lo: float = -0.95, hi: float = 2.0, iterations: int = 120) -> Optional[float]:
    if not cashflows or len(cashflows) < 2:
        return None

    def f(rate: float) -> float:
        s = 0.0
        for i, cf in enumerate(cashflows):
            s += float(cf) / ((1.0 + rate) ** i)
        return s

    flo = f(lo)
    fhi = f(hi)
    if math.isnan(flo) or math.isnan(fhi):
        return None
    if flo == 0.0:
        return lo
    if fhi == 0.0:
        return hi
    if flo * fhi > 0:
        return None

    a, b = lo, hi
    fa, fb = flo, fhi
    for _ in range(iterations):
        mid = (a + b) / 2.0
        fm = f(mid)
        if abs(fm) < 1e-9:
            return mid
        if fa * fm <= 0:
            b = mid
            fb = fm
        else:
            a = mid
            fa = fm
    return (a + b) / 2.0


def payback_year(initial_cost: float, annual_savings: list[float], discount_rate_pct: Optional[float] = None) -> Optional[float]:
    cost = float(max(initial_cost, 0.0))
    if cost <= 0:
        return 0.0
    running = -cost
    rate = None if discount_rate_pct is None else float(discount_rate_pct) / 100.0
    for idx, sav in enumerate(annual_savings, start=1):
        val = float(sav)
        if rate is not None:
            val = val / ((1.0 + rate) ** idx)
        prev = running
        running += val
        if running >= 0:
            if val <= 0:
                return float(idx)
            frac = min(max((-prev) / val, 0.0), 1.0)
            return float(idx - 1 + frac)
    return None


def build_report_markdown(
    run_title: str,
    assumptions: dict,
    results: dict,
    quote_comparison_df: Optional[pd.DataFrame] = None,
) -> str:
    lines = [
        f"# {run_title}",
        f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Assumptions",
    ]
    for k, v in assumptions.items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Results")
    for k, v in results.items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    if isinstance(quote_comparison_df, pd.DataFrame) and not quote_comparison_df.empty:
        lines.append("## Quote Vs Calculated")
        try:
            lines.append(quote_comparison_df.to_markdown(index=False))
        except Exception:
            lines.append(quote_comparison_df.to_csv(index=False))
        lines.append("")
    return "\n".join(lines)


def markdown_to_pdf_bytes(md_text: str) -> Optional[bytes]:
    try:
        from matplotlib.backends.backend_pdf import PdfPages
        import matplotlib.pyplot as plt
    except Exception:
        return None

    txt = str(md_text or "")
    raw_lines = txt.splitlines() if txt else [""]
    wrapped = []
    for line in raw_lines:
        line = str(line)
        if len(line) <= 105:
            wrapped.append(line)
            continue
        start = 0
        while start < len(line):
            wrapped.append(line[start : start + 105])
            start += 105
    if not wrapped:
        wrapped = [""]

    lines_per_page = 58
    pages = max(int(math.ceil(len(wrapped) / float(lines_per_page))), 1)
    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        for p in range(pages):
            fig = plt.figure(figsize=(8.27, 11.69))
            ax = fig.add_axes([0, 0, 1, 1])
            ax.axis("off")
            y = 0.97
            start = p * lines_per_page
            end = min(start + lines_per_page, len(wrapped))
            for line in wrapped[start:end]:
                ax.text(0.04, y, line, ha="left", va="top", fontsize=8.5, family="DejaVu Sans Mono")
                y -= 0.016
            ax.text(0.5, 0.015, f"Page {p + 1}/{pages}", ha="center", va="bottom", fontsize=8, color="#666666")
            pdf.savefig(fig)
            plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


st.set_page_config(page_title="Solar Quote Accuracy Checker", layout="wide")
st.title("Solar Quote Accuracy Checker")
st.caption(
    "Checks quote calculations against your assumptions and interval data, then restates results when variables change. "
    "Designed as a battery-ready framework."
)

with st.sidebar:
    st.header("Inputs")
    quote_pdf = st.file_uploader("Quote PDF", type=["pdf"], key="quote_pdf")
    invoice_pdf = st.file_uploader("Current retailer invoice PDF (optional)", type=["pdf"], key="invoice_pdf")
    nem12_file = st.file_uploader("NEM12 data file (CSV)", type=["csv"], key="nem12")
    solar_file = st.file_uploader("Solar production file (optional)", type=["csv", "xlsx", "xls"], key="solar")

quote_defaults = {}
if quote_pdf is not None:
    if PdfReader is None:
        st.warning("`pypdf` is not installed, so quote fields cannot be auto-extracted. You can still enter values manually.")
    else:
        quote_defaults = extract_quote_defaults_from_pdf_bytes(quote_pdf.getvalue())
        with st.expander("Extracted quote fields", expanded=False):
            st.json({k: v for k, v in quote_defaults.items() if v is not None})

invoice_defaults = {}
if invoice_pdf is not None:
    if PdfReader is None:
        st.warning("`pypdf` is not installed, so invoice fields cannot be auto-extracted. You can still enter values manually.")
    else:
        invoice_defaults = extract_invoice_tariff_defaults_from_pdf_bytes(invoice_pdf.getvalue())
        with st.expander("Extracted invoice tariff fields", expanded=False):
            st.json(
                {
                    "invoice_import_rate_c_per_kwh": invoice_defaults.get("invoice_import_rate_c_per_kwh"),
                    "invoice_controlled_rate_c_per_kwh": invoice_defaults.get("invoice_controlled_rate_c_per_kwh"),
                    "invoice_fit_rate_c_per_kwh": invoice_defaults.get("invoice_fit_rate_c_per_kwh"),
                    "invoice_supply_d_per_day": invoice_defaults.get("invoice_supply_d_per_day"),
                    "invoice_pages": invoice_defaults.get("invoice_pages"),
                    "invoice_text_chars": invoice_defaults.get("invoice_text_chars"),
                    "invoice_text_extract_ok": invoice_defaults.get("invoice_text_extract_ok"),
                    "invoice_ocr_used": invoice_defaults.get("invoice_ocr_used"),
                    "invoice_extract_note": invoice_defaults.get("invoice_extract_note"),
                }
            )
        if not bool(invoice_defaults.get("invoice_text_extract_ok", False)):
            st.warning(
                "Invoice PDF appears to be image-only (no readable text layer), so tariff extraction could not run. "
                "Use manual tariff entry or upload an OCR/text-based invoice PDF."
            )

df_int = pd.DataFrame(columns=["register", "timestamp", "kwh"])
interval_summary = {}
if nem12_file is not None:
    df_int = read_nem12_5min_bytes(nem12_file.getvalue())
    interval_summary = summarize_interval_energy(df_int)

solar_aligned = pd.DataFrame(columns=["timestamp", "pv_kwh"])
solar_match_pct = 0.0
solar_observed = {}
if solar_file is not None and not df_int.empty:
    df_solar = read_solar_profile_bytes(solar_file.name, solar_file.getvalue())
    solar_aligned, solar_match_pct = align_solar_to_intervals(df_solar, df_int)
    solar_observed = estimate_solar_metrics(df_int, solar_aligned)

q = quote_defaults
quoted_default_import_c = (_safe_float(q.get("import_rate_d_per_kwh"), 0.31) * 100.0) if q else 31.0
quoted_default_fit_c = (_safe_float(q.get("fit_d_per_kwh"), 0.07) * 100.0) if q else 7.0
quoted_default_supply_d = _safe_float(q.get("daily_supply_charge_d"), 1.20)
default_system_cost = _safe_float(
    q.get("quote_cost_base_net_stc"),
    _safe_float(q.get("quote_total_cost"), 9790.0),
)

st.subheader("1) Tariffs (quoted vs actual)")
st.caption("Quoted tariffs are used for quote alignment checks. Actual tariffs are used for restated analysis.")

rq1, rq2 = st.columns(2)
with rq1:
    if st.button("Clear quoted tariff inputs (set to 0)"):
        st.session_state["quoted_general_rate_c"] = 0.0
        st.session_state["quoted_controlled_rate_c"] = 0.0
        st.session_state["quoted_fit_rate_c"] = 0.0
        st.session_state["quoted_supply_d"] = 0.0
with rq2:
    if st.button("Clear actual tariff inputs (set to 0)"):
        for _mode in ("manual", "invoice", "quoted"):
            st.session_state[f"actual_general_rate_{_mode}"] = 0.0
            st.session_state[f"actual_controlled_rate_{_mode}"] = 0.0
            st.session_state[f"actual_fit_rate_{_mode}"] = 0.0
            st.session_state[f"actual_supply_d_{_mode}"] = 0.0

qt1, qt2, qt3, qt4 = st.columns(4)
with qt1:
    quoted_general_rate_c = st.number_input(
        "Quoted general import rate (c/kWh)",
        min_value=0.0,
        value=float(quoted_default_import_c),
        step=0.1,
        key="quoted_general_rate_c",
    )
with qt2:
    quoted_controlled_rate_c = st.number_input(
        "Quoted controlled load rate (c/kWh)",
        min_value=0.0,
        value=float(quoted_default_import_c),
        step=0.1,
        key="quoted_controlled_rate_c",
    )
with qt3:
    quoted_fit_rate_c = st.number_input(
        "Quoted feed-in tariff (c/kWh)",
        min_value=0.0,
        value=float(quoted_default_fit_c),
        step=0.1,
        key="quoted_fit_rate_c",
    )
with qt4:
    quoted_supply_d = st.number_input(
        "Quoted daily supply charge ($/day)",
        min_value=0.0,
        value=float(quoted_default_supply_d),
        step=0.01,
        key="quoted_supply_d",
    )

invoice_has_any = any(
    invoice_defaults.get(k) is not None
    for k in (
        "invoice_import_rate_c_per_kwh",
        "invoice_controlled_rate_c_per_kwh",
        "invoice_fit_rate_c_per_kwh",
        "invoice_supply_d_per_day",
    )
)
actual_tariff_source = st.radio(
    "Actual tariff source",
    ["Manual entry", "Invoice prefill", "Use quoted tariff"],
    horizontal=True,
    help="Invoice prefill populates defaults from uploaded invoice text; all values remain editable.",
)
if actual_tariff_source == "Invoice prefill" and not invoice_has_any:
    st.info(
        "No invoice tariff values were detected, so nothing was imported from the invoice. "
        "Enter actual tariff values manually below."
    )

if actual_tariff_source == "Invoice prefill" and invoice_has_any:
    actual_default_import_c = _safe_float(invoice_defaults.get("invoice_import_rate_c_per_kwh"), quoted_general_rate_c)
    actual_default_controlled_c = _safe_float(invoice_defaults.get("invoice_controlled_rate_c_per_kwh"), quoted_controlled_rate_c)
    actual_default_fit_c = _safe_float(invoice_defaults.get("invoice_fit_rate_c_per_kwh"), quoted_fit_rate_c)
    actual_default_supply_d = _safe_float(invoice_defaults.get("invoice_supply_d_per_day"), quoted_supply_d)
elif actual_tariff_source == "Use quoted tariff":
    actual_default_import_c = float(quoted_general_rate_c)
    actual_default_controlled_c = float(quoted_controlled_rate_c)
    actual_default_fit_c = float(quoted_fit_rate_c)
    actual_default_supply_d = float(quoted_supply_d)
else:
    # Manual mode starts blank-ish (0.0) so users can enter their real tariff values cleanly.
    actual_default_import_c = 0.0
    actual_default_controlled_c = 0.0
    actual_default_fit_c = 0.0
    actual_default_supply_d = 0.0

source_key = {
    "Manual entry": "manual",
    "Invoice prefill": "invoice",
    "Use quoted tariff": "quoted",
}[actual_tariff_source]
disable_actual = actual_tariff_source == "Use quoted tariff"

actual_key_map = {
    "general": f"actual_general_rate_{source_key}",
    "controlled": f"actual_controlled_rate_{source_key}",
    "fit": f"actual_fit_rate_{source_key}",
    "supply": f"actual_supply_d_{source_key}",
}
if actual_key_map["general"] not in st.session_state:
    st.session_state[actual_key_map["general"]] = float(actual_default_import_c)
if actual_key_map["controlled"] not in st.session_state:
    st.session_state[actual_key_map["controlled"]] = float(actual_default_controlled_c)
if actual_key_map["fit"] not in st.session_state:
    st.session_state[actual_key_map["fit"]] = float(actual_default_fit_c)
if actual_key_map["supply"] not in st.session_state:
    st.session_state[actual_key_map["supply"]] = float(actual_default_supply_d)

# Keep "Use quoted tariff" truly linked to quoted inputs.
if actual_tariff_source == "Use quoted tariff":
    st.session_state[actual_key_map["general"]] = float(quoted_general_rate_c)
    st.session_state[actual_key_map["controlled"]] = float(quoted_controlled_rate_c)
    st.session_state[actual_key_map["fit"]] = float(quoted_fit_rate_c)
    st.session_state[actual_key_map["supply"]] = float(quoted_supply_d)

at1, at2, at3, at4 = st.columns(4)
with at1:
    general_rate_c = st.number_input(
        "Actual general import rate (c/kWh)",
        min_value=0.0,
        value=float(actual_default_import_c),
        step=0.1,
        disabled=disable_actual,
        key=f"actual_general_rate_{source_key}",
    )
with at2:
    controlled_rate_c = st.number_input(
        "Actual controlled load rate (c/kWh)",
        min_value=0.0,
        value=float(actual_default_controlled_c),
        step=0.1,
        disabled=disable_actual,
        key=f"actual_controlled_rate_{source_key}",
    )
with at3:
    fit_rate_c = st.number_input(
        "Actual feed-in tariff (c/kWh)",
        min_value=0.0,
        value=float(actual_default_fit_c),
        step=0.1,
        disabled=disable_actual,
        key=f"actual_fit_rate_{source_key}",
    )
with at4:
    supply_d = st.number_input(
        "Actual daily supply charge ($/day)",
        min_value=0.0,
        value=float(actual_default_supply_d),
        step=0.01,
        disabled=disable_actual,
        key=f"actual_supply_d_{source_key}",
    )

quoted_plan = FlatPlan(
    general_c_per_kwh=float(quoted_general_rate_c),
    controlled_c_per_kwh=float(quoted_controlled_rate_c),
    feed_in_c_per_kwh=float(quoted_fit_rate_c),
    supply_d_per_day=float(quoted_supply_d),
)
plan_input = FlatPlan(
    general_c_per_kwh=float(general_rate_c),
    controlled_c_per_kwh=float(controlled_rate_c),
    feed_in_c_per_kwh=float(fit_rate_c),
    supply_d_per_day=float(supply_d),
)

tariff_label = st.radio(
    "Tariff structure for interval billing",
    ["Flat", "TOU"],
    horizontal=True,
    help="Quote-style model remains flat-rate. Interval detailed model uses this tariff.",
)
tariff_mode = "tou" if tariff_label == "TOU" else "flat"
day_label_to_mode = {"All days": "all", "Weekdays": "wkday", "Weekends": "wkend"}
mode_to_day_label = {v: k for k, v in day_label_to_mode.items()}

tou_bands: list[TouBand] = []
if tariff_mode == "tou":
    st.caption(
        "TOU bands are applied in order: Peak -> Shoulder -> Off-peak. "
        "Overnight windows are supported when start > end (for example, 21:00 to 09:00)."
    )
    t1, t2, t3, t4 = st.columns(4)
    with t1:
        peak_rate_c = st.number_input("Peak rate (c/kWh)", min_value=0.0, value=float(max(general_rate_c + 8.0, 0.0)), step=0.1)
    with t2:
        peak_days_label = st.selectbox("Peak days", list(day_label_to_mode.keys()), index=1)
    with t3:
        peak_start = st.text_input("Peak start (HH:MM)", value="16:00")
    with t4:
        peak_end = st.text_input("Peak end (HH:MM)", value="21:00")

    t5, t6, t7, t8 = st.columns(4)
    with t5:
        shoulder_rate_c = st.number_input("Shoulder rate (c/kWh)", min_value=0.0, value=float(max(general_rate_c, 0.0)), step=0.1)
    with t6:
        shoulder_days_label = st.selectbox("Shoulder days", list(day_label_to_mode.keys()), index=1)
    with t7:
        shoulder_start = st.text_input("Shoulder start (HH:MM)", value="07:00")
    with t8:
        shoulder_end = st.text_input("Shoulder end (HH:MM)", value="16:00")

    t9, t10, t11, t12 = st.columns(4)
    with t9:
        offpeak_rate_c = st.number_input("Off-peak rate (c/kWh)", min_value=0.0, value=float(max(general_rate_c - 8.0, 0.0)), step=0.1)
    with t10:
        offpeak_days_label = st.selectbox("Off-peak days", list(day_label_to_mode.keys()), index=0)
    with t11:
        offpeak_start = st.text_input("Off-peak start (HH:MM)", value="22:00")
    with t12:
        offpeak_end = st.text_input("Off-peak end (HH:MM)", value="07:00")

    tou_bands = [
        TouBand("Peak", float(peak_rate_c), day_label_to_mode[str(peak_days_label)], str(peak_start), str(peak_end)),
        TouBand("Shoulder", float(shoulder_rate_c), day_label_to_mode[str(shoulder_days_label)], str(shoulder_start), str(shoulder_end)),
        TouBand("Off-peak", float(offpeak_rate_c), day_label_to_mode[str(offpeak_days_label)], str(offpeak_start), str(offpeak_end)),
    ]

demand_enabled = st.checkbox("Enable demand charge in interval billing", value=False)
dm1, dm2, dm3, dm4 = st.columns(4)
with dm1:
    demand_rate_c_kw_day = st.number_input(
        "Demand rate (c/kW/day)",
        min_value=0.0,
        value=0.0,
        step=0.1,
        disabled=not demand_enabled,
    )
with dm2:
    demand_days_label = st.selectbox("Demand days", list(day_label_to_mode.keys()), index=1, disabled=not demand_enabled)
with dm3:
    demand_start_hhmm = st.text_input("Demand start (HH:MM)", value="16:00", disabled=not demand_enabled)
with dm4:
    demand_end_hhmm = st.text_input("Demand end (HH:MM)", value="21:00", disabled=not demand_enabled)

tariff_cfg = TariffConfig(
    mode=tariff_mode,
    flat_import_c_per_kwh=float(plan_input.general_c_per_kwh),
    controlled_c_per_kwh=float(plan_input.controlled_c_per_kwh),
    fit_c_per_kwh=float(plan_input.feed_in_c_per_kwh),
    supply_d_per_day=float(plan_input.supply_d_per_day),
    tou_bands=tou_bands,
    demand_enabled=bool(demand_enabled),
    demand_c_per_kw_day=float(demand_rate_c_kw_day),
    demand_days=day_label_to_mode[str(demand_days_label)],
    demand_start_hhmm=str(demand_start_hhmm),
    demand_end_hhmm=str(demand_end_hhmm),
)

if tariff_mode == "tou":
    actual_effective_import_c = _effective_tou_import_rate_c_per_kwh(
        bands=tou_bands,
        fallback_c=float(plan_input.general_c_per_kwh),
    )
else:
    actual_effective_import_c = float(plan_input.general_c_per_kwh)

plan = FlatPlan(
    general_c_per_kwh=float(actual_effective_import_c),
    controlled_c_per_kwh=float(plan_input.controlled_c_per_kwh),
    feed_in_c_per_kwh=float(plan_input.feed_in_c_per_kwh),
    supply_d_per_day=float(plan_input.supply_d_per_day),
)

if tariff_mode == "tou":
    st.caption(
        f"Tariff comparison and quote-style restatement use TOU effective import rate: "
        f"**{actual_effective_import_c:.2f} c/kWh** (time-weighted)."
    )

import_label = "Import effective from TOU (c/kWh)" if tariff_mode == "tou" else "General import (c/kWh)"
tariff_compare_rows = [
    {
        "Tariff component": import_label,
        "Quoted": float(quoted_plan.general_c_per_kwh),
        "Actual": float(plan.general_c_per_kwh),
    },
    {
        "Tariff component": "Controlled load (c/kWh)",
        "Quoted": float(quoted_plan.controlled_c_per_kwh),
        "Actual": float(plan.controlled_c_per_kwh),
    },
    {
        "Tariff component": "Feed-in tariff (c/kWh)",
        "Quoted": float(quoted_plan.feed_in_c_per_kwh),
        "Actual": float(plan.feed_in_c_per_kwh),
    },
    {
        "Tariff component": "Daily supply ($/day)",
        "Quoted": float(quoted_plan.supply_d_per_day),
        "Actual": float(plan.supply_d_per_day),
    },
]
tariff_compare_df = pd.DataFrame(tariff_compare_rows)
tariff_compare_df["Delta (Actual - Quoted)"] = tariff_compare_df["Actual"] - tariff_compare_df["Quoted"]
st.dataframe(tariff_compare_df, use_container_width=True)

st.subheader("2) Quote and scenario variables")
sv1, sv2, sv3, sv4 = st.columns(4)
with sv1:
    system_size_kwdc = st.number_input(
        "Solar system size (kW DC)",
        min_value=0.0,
        value=float(_safe_float(q.get("system_size_kwdc"), 13.2)),
        step=0.1,
    )
with sv2:
    ac_size_kw = st.number_input(
        "Inverter size (kW AC)",
        min_value=0.0,
        value=float(_safe_float(q.get("ac_system_size_kw"), 10.0)),
        step=0.1,
    )
with sv3:
    annual_production_kwh = st.number_input(
        "Annual production (kWh)",
        min_value=0.0,
        value=float(_safe_float(q.get("annual_production_kwh"), 21831.0)),
        step=100.0,
    )
with sv4:
    self_consumption_pct = st.number_input(
        "Self-consumption (%)",
        min_value=0.0,
        max_value=100.0,
        value=float(_safe_float(q.get("self_consumption_pct"), 40.0)),
        step=1.0,
    )

if solar_observed:
    st.caption(
        f"Uploaded solar alignment: {solar_match_pct:.1f}% of NEM12 intervals. "
        f"Observed PV={solar_observed.get('pv_generated_kwh', 0.0):,.1f} kWh, "
        f"observed self-consumption={solar_observed.get('self_consumption_pct', 0.0):.1f}%."
    )
    use_observed_solar = st.checkbox("Match annual production + self-consumption to uploaded profile", value=False)
    if use_observed_solar:
        annual_production_kwh = float(solar_observed.get("pv_generated_kwh", annual_production_kwh))
        self_consumption_pct = float(solar_observed.get("self_consumption_pct", self_consumption_pct))

solar_profile_mode = "Modelled from annual production"
if not df_int.empty:
    profile_options = ["Modelled from annual production"]
    if not solar_aligned.empty:
        profile_options.append("Uploaded profile scaled to annual production")
        profile_options.append("Uploaded profile as uploaded")
    solar_profile_mode = st.radio("Interval solar profile source", profile_options, horizontal=True)
    apply_inverter_cap = st.checkbox(
        "Apply inverter AC cap to interval PV profile",
        value=False,
        help="Use this only when you want to test clipping from DC/AC sizing.",
    )
else:
    apply_inverter_cap = False

use_modelled_self_consumption_for_quote = st.checkbox(
    "Use interval-modelled self-consumption for quote-style solar calculation",
    value=False,
    disabled=df_int.empty,
)

ev1, ev2, ev3, ev4, ev5 = st.columns(5)
with ev1:
    system_cost = st.number_input(
        "Solar installed cost ($)",
        min_value=0.0,
        value=float(default_system_cost),
        step=100.0,
    )
with ev2:
    lifetime_years = int(
        st.number_input(
            "Lifetime (years)",
            min_value=1,
            max_value=40,
            value=int(_safe_float(q.get("lifetime_years"), 20)),
            step=1,
        )
    )
with ev3:
    utility_inflation_pct = st.number_input(
        "Utility inflation (%/yr)",
        min_value=0.0,
        value=float(_safe_float(q.get("utility_inflation_pct"), 2.75)),
        step=0.05,
    )
with ev4:
    discount_rate_pct = st.number_input(
        "Discount rate (%/yr)",
        min_value=0.0,
        value=float(_safe_float(q.get("discount_rate_pct"), 1.36)),
        step=0.05,
    )
with ev5:
    first_year_perf_pct = st.number_input(
        "PV output in year 1 (% of nameplate)",
        min_value=0.0,
        max_value=120.0,
        value=float(_safe_float(q.get("pv_degradation_first_year_pct"), 99.0)),
        step=0.1,
    )

deg_col, _ = st.columns([1, 4])
with deg_col:
    annual_degradation_pct = st.number_input(
        "Annual PV degradation (%/yr)",
        min_value=0.0,
        max_value=5.0,
        value=float(_safe_float(q.get("pv_degradation_annual_pct"), 0.4)),
        step=0.05,
    )

st.subheader("3) Baseline source")
baseline_options = []
if interval_summary:
    baseline_options.append("NEM12 annualised")
if q.get("quoted_quarterly_bill_assumption") is not None:
    baseline_options.append("Quote quarterly bill assumption")
if not baseline_options:
    baseline_options = ["Manual annual bill"]

baseline_source = st.radio("Baseline method", baseline_options, horizontal=True)
assume_no_existing_export = False
treat_existing_export_as_load = False
manual_annual_bill = 0.0

if baseline_source == "NEM12 annualised":
    b1, b2 = st.columns(2)
    with b1:
        assume_no_existing_export = st.checkbox("Assume no existing solar/export in baseline", value=True)
    with b2:
        treat_existing_export_as_load = st.checkbox(
            "If true, add existing export back into baseline import",
            value=True,
            help="Useful when the uploaded data already includes legacy solar and you want a no-solar baseline.",
        )
elif baseline_source == "Quote quarterly bill assumption":
    st.caption("Uses the quote's quarterly bill assumption, daily supply charge, and actual entered import rate.")
else:
    manual_annual_bill = st.number_input("Manual baseline annual bill ($/yr)", min_value=0.0, value=2286.62, step=50.0)

st.subheader("4) Future battery quote (optional)")
use_battery = st.checkbox("Enable battery scenario")
battery_model_mode = "Proxy (quote-style)"
if use_battery:
    battery_model_mode = st.radio(
        "Battery model",
        ["Proxy (quote-style)", "Dispatch (interval)"],
        horizontal=True,
    )

bc1, bc2, bc3 = st.columns(3)
with bc1:
    battery_capture_pct = st.number_input(
        "Battery captures export (%)",
        min_value=0.0,
        max_value=100.0,
        value=60.0,
        step=1.0,
        disabled=not use_battery,
        help="Proxy for how much exported solar the battery can absorb and shift.",
    )
with bc2:
    battery_eff_pct = st.number_input(
        "Battery roundtrip efficiency (%)",
        min_value=0.0,
        max_value=100.0,
        value=90.0,
        step=1.0,
        disabled=not use_battery,
    )
with bc3:
    battery_cost = st.number_input(
        "Battery installed cost ($)",
        min_value=0.0,
        value=10000.0,
        step=250.0,
        disabled=not use_battery,
    )

db1, db2, db3, db4, db5 = st.columns(5)
with db1:
    battery_capacity_kwh = st.number_input(
        "Dispatch capacity (kWh)",
        min_value=0.0,
        value=10.0,
        step=0.5,
        disabled=(not use_battery) or (battery_model_mode != "Dispatch (interval)"),
    )
with db2:
    battery_power_kw = st.number_input(
        "Dispatch power (kW)",
        min_value=0.0,
        value=5.0,
        step=0.5,
        disabled=(not use_battery) or (battery_model_mode != "Dispatch (interval)"),
    )
with db3:
    battery_reserve_pct = st.number_input(
        "Reserve SOC (%)",
        min_value=0.0,
        max_value=100.0,
        value=10.0,
        step=1.0,
        disabled=(not use_battery) or (battery_model_mode != "Dispatch (interval)"),
    )
with db4:
    battery_init_soc_pct = st.number_input(
        "Initial SOC (%)",
        min_value=0.0,
        max_value=100.0,
        value=50.0,
        step=1.0,
        disabled=(not use_battery) or (battery_model_mode != "Dispatch (interval)"),
    )
with db5:
    battery_discharge_peak_only = st.checkbox(
        "Dispatch only in peak TOU windows",
        value=True,
        disabled=(not use_battery) or (battery_model_mode != "Dispatch (interval)"),
    )

battery_cfg = BatteryAssumptions(
    enabled=bool(use_battery),
    capture_export_pct=float(battery_capture_pct),
    roundtrip_eff_pct=float(battery_eff_pct),
    installed_cost=float(battery_cost if use_battery else 0.0),
)


def _build_baseline() -> tuple[float, float, float, float]:
    if baseline_source == "NEM12 annualised" and interval_summary:
        g = float(interval_summary.get("annual_general_kwh", 0.0))
        c = float(interval_summary.get("annual_controlled_kwh", 0.0))
        e = float(interval_summary.get("annual_export_kwh", 0.0))
        if assume_no_existing_export:
            if treat_existing_export_as_load:
                g += e
            e = 0.0
        bill = annual_bill_from_energy(g, c, e, plan)
        return g, c, e, bill

    if baseline_source == "Quote quarterly bill assumption" and q.get("quoted_quarterly_bill_assumption") is not None:
        g, c, e = derive_usage_from_quarterly_bill(float(q.get("quoted_quarterly_bill_assumption") or 0.0), plan)
        bill = annual_bill_from_energy(g, c, e, plan)
        return g, c, e, bill

    if baseline_source == "Manual annual bill":
        annual_bill = float(max(manual_annual_bill, 0.0))
    else:
        annual_bill = float(_safe_float(q.get("quoted_annual_bill_before"), 0.0))

    annual_supply = plan.supply_d_per_day * 365.0
    usage_component = max(annual_bill - annual_supply, 0.0)
    g = usage_component / max(plan.general_c_per_kwh / 100.0, 1e-9)
    return g, 0.0, 0.0, annual_bill_from_energy(g, 0.0, 0.0, plan)


interval_detail_available = not df_int.empty
interval_notes: list[str] = []
interval_bill_base: Optional[dict] = None
interval_bill_solar: Optional[dict] = None
interval_bill_final: Optional[dict] = None
interval_solar_meta = {
    "pv_generated_kwh": 0.0,
    "self_consumed_kwh": 0.0,
    "exported_pv_kwh": 0.0,
    "solar_coverage_pct": 0.0,
}
interval_battery_meta = {
    "enabled": False,
    "battery_charge_kwh": 0.0,
    "battery_discharge_kwh": 0.0,
    "import_reduction_kwh": 0.0,
    "export_reduction_kwh": 0.0,
    "end_soc_kwh": 0.0,
}
modelled_self_consumption_pct: Optional[float] = None

if interval_detail_available:
    wide_base_interval = _intervals_wide_from_long(df_int)
    if assume_no_existing_export:
        if treat_existing_export_as_load:
            wide_base_interval["general_kwh"] = (
                pd.to_numeric(wide_base_interval["general_kwh"], errors="coerce").fillna(0.0)
                + pd.to_numeric(wide_base_interval["export_kwh"], errors="coerce").fillna(0.0)
            )
        wide_base_interval["export_kwh"] = 0.0

    if solar_profile_mode == "Uploaded profile scaled to annual production" and not solar_aligned.empty:
        pv_profile_interval = rescale_profile_to_annual_target(solar_aligned, float(annual_production_kwh), df_int)
    elif solar_profile_mode == "Uploaded profile as uploaded" and not solar_aligned.empty:
        pv_profile_interval = solar_aligned[["timestamp", "pv_kwh"]].copy()
    else:
        pv_profile_interval = build_profile_from_annual_target(df_int, float(annual_production_kwh))

    if apply_inverter_cap and not pv_profile_interval.empty and ac_size_kw > 0:
        dt_minutes = _interval_minutes(wide_base_interval["timestamp"])
        cap_kwh = float(ac_size_kw) * (dt_minutes / 60.0)
        before_cap = float(pd.to_numeric(pv_profile_interval["pv_kwh"], errors="coerce").fillna(0.0).sum())
        pv_profile_interval["pv_kwh"] = (
            pd.to_numeric(pv_profile_interval["pv_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=cap_kwh)
        )
        after_cap = float(pv_profile_interval["pv_kwh"].sum())
        if after_cap + 1e-9 < before_cap:
            interval_notes.append(f"Inverter cap clipped interval PV by {(before_cap - after_cap):,.1f} kWh over the loaded period.")

    wide_after_solar_interval, interval_solar_meta = apply_pv_to_wide(
        wide_base_interval,
        pv_profile_interval,
        assume_no_existing_export=False,
        add_existing_export_to_general=False,
    )
    interval_bill_base = simulate_bill_interval(wide_base_interval, tariff_cfg)
    interval_bill_solar = simulate_bill_interval(wide_after_solar_interval, tariff_cfg)
    wide_after_final_interval = wide_after_solar_interval.copy()

    if use_battery and battery_model_mode == "Dispatch (interval)":
        peak_band_exists = any(str(b.name).lower().startswith("peak") for b in tariff_cfg.tou_bands)
        discharge_peak_only_eff = bool(battery_discharge_peak_only and peak_band_exists)
        if battery_discharge_peak_only and not peak_band_exists:
            interval_notes.append("Peak-only dispatch requested, but no peak TOU band exists. Dispatch used all hours.")
        wide_after_final_interval, interval_battery_meta = apply_battery_dispatch_to_wide(
            wide_in=wide_after_solar_interval,
            capacity_kwh=float(battery_capacity_kwh),
            power_kw=float(battery_power_kw),
            roundtrip_eff_pct=float(battery_eff_pct),
            reserve_pct=float(battery_reserve_pct),
            init_soc_pct=float(battery_init_soc_pct),
            discharge_peak_only=discharge_peak_only_eff,
            tou_bands=tariff_cfg.tou_bands,
        )
    elif use_battery and battery_model_mode != "Dispatch (interval)":
        interval_notes.append("Proxy battery mode does not alter interval detailed results; interval final equals solar-only.")

    interval_bill_final = simulate_bill_interval(wide_after_final_interval, tariff_cfg)
    pv_total = float(interval_solar_meta.get("pv_generated_kwh", 0.0))
    sc_kwh = float(interval_solar_meta.get("self_consumed_kwh", 0.0))
    modelled_self_consumption_pct = (sc_kwh / pv_total * 100.0) if pv_total > 0 else 0.0
else:
    interval_notes.append("Upload NEM12 interval data to enable TOU/demand and dispatch battery calculations.")

self_consumption_pct_quote = float(self_consumption_pct)
if use_modelled_self_consumption_for_quote and modelled_self_consumption_pct is not None:
    self_consumption_pct_quote = float(modelled_self_consumption_pct)

base_general_kwh, base_controlled_kwh, base_export_kwh, _baseline_seed_bill = _build_baseline()
annual_bill_before = annual_bill_from_energy(
    base_general_kwh,
    base_controlled_kwh,
    base_export_kwh,
    plan,
)
annual_bill_before_quoted_tariff = annual_bill_from_energy(
    base_general_kwh,
    base_controlled_kwh,
    base_export_kwh,
    quoted_plan,
)
solar_case = apply_solar_quote_model(
    base_general_kwh=base_general_kwh,
    base_controlled_kwh=base_controlled_kwh,
    base_export_kwh=base_export_kwh,
    annual_production_kwh=annual_production_kwh,
    self_consumption_pct=self_consumption_pct_quote,
)
annual_bill_after_solar = annual_bill_from_energy(
    solar_case["general_kwh"],
    solar_case["controlled_kwh"],
    solar_case["export_kwh"],
    plan,
)
annual_bill_after_solar_quoted_tariff = annual_bill_from_energy(
    solar_case["general_kwh"],
    solar_case["controlled_kwh"],
    solar_case["export_kwh"],
    quoted_plan,
)
annual_savings_solar_only = annual_bill_before - annual_bill_after_solar
annual_savings_solar_only_quoted_tariff = annual_bill_before_quoted_tariff - annual_bill_after_solar_quoted_tariff

battery_case = apply_battery_overlay(
    general_kwh=solar_case["general_kwh"],
    controlled_kwh=solar_case["controlled_kwh"],
    export_kwh=solar_case["export_kwh"],
    batt=battery_cfg,
)
annual_bill_after_final = annual_bill_from_energy(
    battery_case["general_kwh"],
    battery_case["controlled_kwh"],
    battery_case["export_kwh"],
    plan,
)
annual_bill_after_final_quoted_tariff = annual_bill_from_energy(
    battery_case["general_kwh"],
    battery_case["controlled_kwh"],
    battery_case["export_kwh"],
    quoted_plan,
)
annual_savings_final = annual_bill_before - annual_bill_after_final
annual_savings_final_quoted_tariff = annual_bill_before_quoted_tariff - annual_bill_after_final_quoted_tariff

tariff_delta_baseline_bill = annual_bill_before - annual_bill_before_quoted_tariff
tariff_delta_solar_bill = annual_bill_after_solar - annual_bill_after_solar_quoted_tariff
tariff_delta_solar_savings = annual_savings_solar_only - annual_savings_solar_only_quoted_tariff
tariff_delta_final_bill = annual_bill_after_final - annual_bill_after_final_quoted_tariff
tariff_delta_final_savings = annual_savings_final - annual_savings_final_quoted_tariff

solar_yearly_savings = build_annual_savings_series(
    year1_savings=annual_savings_solar_only,
    years=lifetime_years,
    utility_inflation_pct=utility_inflation_pct,
    first_year_perf_pct=first_year_perf_pct,
    annual_degradation_pct=annual_degradation_pct,
)
solar_cashflows = [-float(system_cost)] + solar_yearly_savings
solar_npv = npv(solar_cashflows, discount_rate_pct=discount_rate_pct)
solar_roi_pct = ((sum(solar_yearly_savings) - float(system_cost)) / max(float(system_cost), 1e-9)) * 100.0 if system_cost > 0 else 0.0
solar_irr = irr_bisection(solar_cashflows)
solar_payback_discounted = payback_year(system_cost, solar_yearly_savings, discount_rate_pct=discount_rate_pct)

solar_yearly_savings_quoted_tariff = build_annual_savings_series(
    year1_savings=annual_savings_solar_only_quoted_tariff,
    years=lifetime_years,
    utility_inflation_pct=utility_inflation_pct,
    first_year_perf_pct=first_year_perf_pct,
    annual_degradation_pct=annual_degradation_pct,
)
solar_cashflows_quoted_tariff = [-float(system_cost)] + solar_yearly_savings_quoted_tariff
solar_npv_quoted_tariff = npv(solar_cashflows_quoted_tariff, discount_rate_pct=discount_rate_pct)
solar_roi_pct_quoted_tariff = (
    (sum(solar_yearly_savings_quoted_tariff) - float(system_cost)) / max(float(system_cost), 1e-9) * 100.0
    if system_cost > 0
    else 0.0
)
solar_irr_quoted_tariff = irr_bisection(solar_cashflows_quoted_tariff)
solar_payback_discounted_quoted_tariff = payback_year(
    system_cost,
    solar_yearly_savings_quoted_tariff,
    discount_rate_pct=discount_rate_pct,
)

total_upfront_cost = float(system_cost + (battery_cfg.installed_cost if battery_cfg.enabled else 0.0))
final_yearly_savings = build_annual_savings_series(
    year1_savings=annual_savings_final,
    years=lifetime_years,
    utility_inflation_pct=utility_inflation_pct,
    first_year_perf_pct=first_year_perf_pct,
    annual_degradation_pct=annual_degradation_pct,
)
final_yearly_savings_quoted_tariff = build_annual_savings_series(
    year1_savings=annual_savings_final_quoted_tariff,
    years=lifetime_years,
    utility_inflation_pct=utility_inflation_pct,
    first_year_perf_pct=first_year_perf_pct,
    annual_degradation_pct=annual_degradation_pct,
)
final_cashflows = [-total_upfront_cost] + final_yearly_savings
final_npv = npv(final_cashflows, discount_rate_pct=discount_rate_pct)
final_roi_pct = ((sum(final_yearly_savings) - total_upfront_cost) / max(total_upfront_cost, 1e-9)) * 100.0 if total_upfront_cost > 0 else 0.0
final_irr = irr_bisection(final_cashflows)
final_payback_discounted = payback_year(total_upfront_cost, final_yearly_savings, discount_rate_pct=discount_rate_pct)

interval_annual_savings_solar: Optional[float] = None
interval_annual_savings_final: Optional[float] = None
interval_solar_yearly_savings = [float("nan")] * lifetime_years
interval_final_yearly_savings = [float("nan")] * lifetime_years
interval_solar_npv: Optional[float] = None
interval_solar_roi_pct: Optional[float] = None
interval_solar_irr_pct: Optional[float] = None
interval_solar_payback: Optional[float] = None
interval_final_npv: Optional[float] = None
interval_final_roi_pct: Optional[float] = None
interval_final_irr_pct: Optional[float] = None
interval_final_payback: Optional[float] = None

if interval_bill_base and interval_bill_solar and interval_bill_final:
    interval_annual_savings_solar = float(interval_bill_base["annual_bill"] - interval_bill_solar["annual_bill"])
    interval_annual_savings_final = float(interval_bill_base["annual_bill"] - interval_bill_final["annual_bill"])
    interval_solar_yearly_savings = build_annual_savings_series(
        year1_savings=interval_annual_savings_solar,
        years=lifetime_years,
        utility_inflation_pct=utility_inflation_pct,
        first_year_perf_pct=first_year_perf_pct,
        annual_degradation_pct=annual_degradation_pct,
    )
    interval_solar_cashflows = [-float(system_cost)] + interval_solar_yearly_savings
    interval_solar_npv = npv(interval_solar_cashflows, discount_rate_pct=discount_rate_pct)
    interval_solar_roi_pct = ((sum(interval_solar_yearly_savings) - float(system_cost)) / max(float(system_cost), 1e-9)) * 100.0 if system_cost > 0 else 0.0
    interval_solar_irr = irr_bisection(interval_solar_cashflows)
    interval_solar_irr_pct = (interval_solar_irr * 100.0) if interval_solar_irr is not None else None
    interval_solar_payback = payback_year(system_cost, interval_solar_yearly_savings, discount_rate_pct=discount_rate_pct)

    interval_final_yearly_savings = build_annual_savings_series(
        year1_savings=interval_annual_savings_final,
        years=lifetime_years,
        utility_inflation_pct=utility_inflation_pct,
        first_year_perf_pct=first_year_perf_pct,
        annual_degradation_pct=annual_degradation_pct,
    )
    interval_final_cashflows = [-total_upfront_cost] + interval_final_yearly_savings
    interval_final_npv = npv(interval_final_cashflows, discount_rate_pct=discount_rate_pct)
    interval_final_roi_pct = ((sum(interval_final_yearly_savings) - total_upfront_cost) / max(total_upfront_cost, 1e-9)) * 100.0 if total_upfront_cost > 0 else 0.0
    interval_final_irr = irr_bisection(interval_final_cashflows)
    interval_final_irr_pct = (interval_final_irr * 100.0) if interval_final_irr is not None else None
    interval_final_payback = payback_year(total_upfront_cost, interval_final_yearly_savings, discount_rate_pct=discount_rate_pct)

st.subheader("5) Results")
tab_quote, tab_interval = st.tabs(["Quote-style annual model", "Interval detailed model"])

with tab_quote:
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Baseline annual bill", _currency(annual_bill_before))
    r2.metric("Solar-only annual bill", _currency(annual_bill_after_solar))
    r3.metric("Solar-only annual savings", _currency(annual_savings_solar_only))
    if use_battery:
        r4.metric("Solar + battery annual savings", _currency(annual_savings_final))
    else:
        r4.metric("Self-consumption used", _pct(self_consumption_pct_quote))

    st.markdown("**Tariff variance impact (Actual vs Quoted)**")
    tv1, tv2, tv3, tv4 = st.columns(4)
    tv1.metric("Baseline bill (quoted tariff)", _currency(annual_bill_before_quoted_tariff))
    tv2.metric(
        "Baseline bill (actual tariff)",
        _currency(annual_bill_before),
        delta=_currency_delta(tariff_delta_baseline_bill),
    )
    tv3.metric("Solar savings (quoted tariff)", _currency(annual_savings_solar_only_quoted_tariff))
    tv4.metric(
        "Solar savings (actual tariff)",
        _currency(annual_savings_solar_only),
        delta=_currency_delta(tariff_delta_solar_savings),
    )
    if use_battery:
        tvb1, tvb2 = st.columns(2)
        tvb1.metric("Final savings (quoted tariff)", _currency(annual_savings_final_quoted_tariff))
        tvb2.metric(
            "Final savings (actual tariff)",
            _currency(annual_savings_final),
            delta=_currency_delta(tariff_delta_final_savings),
        )

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Baseline import (kWh/yr)", f"{base_general_kwh + base_controlled_kwh:,.0f}")
    d2.metric("Post-solar import (kWh/yr)", f"{solar_case['general_kwh'] + solar_case['controlled_kwh']:,.0f}")
    d3.metric("Post-solar export (kWh/yr)", f"{solar_case['export_kwh']:,.0f}")
    d4.metric("Solar self-consumed (kWh/yr)", f"{solar_case['self_consumed_kwh']:,.0f}")

    if use_modelled_self_consumption_for_quote and modelled_self_consumption_pct is not None:
        st.caption(f"Quote-style self-consumption overridden by interval model: {modelled_self_consumption_pct:.1f}%")
    elif modelled_self_consumption_pct is not None:
        st.caption(f"Interval-modelled self-consumption (reference): {modelled_self_consumption_pct:.1f}%")

    if use_battery:
        st.caption(
            f"Quote-style battery proxy captured {battery_case['battery_captured_export_kwh']:,.0f} kWh/yr of export "
            f"and delivered {battery_case['battery_delivered_kwh']:,.0f} kWh/yr to reduce imports."
        )
        if battery_model_mode == "Dispatch (interval)":
            st.caption("Quote-style path still uses proxy battery logic; dispatch applies only in interval detailed tab.")

    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Solar NPV", _currency(solar_npv))
    e2.metric("Solar ROI", _pct(solar_roi_pct))
    e3.metric("Solar IRR", _pct((solar_irr or 0.0) * 100.0) if solar_irr is not None else "-")
    e4.metric("Solar discounted payback", (f"{solar_payback_discounted:.1f} years" if solar_payback_discounted is not None else "-"))

    if use_battery:
        st.markdown("**Solar + battery economics**")
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Combined NPV", _currency(final_npv))
        f2.metric("Combined ROI", _pct(final_roi_pct))
        f3.metric("Combined IRR", _pct((final_irr or 0.0) * 100.0) if final_irr is not None else "-")
        f4.metric("Combined discounted payback", (f"{final_payback_discounted:.1f} years" if final_payback_discounted is not None else "-"))

with tab_interval:
    if not interval_detail_available or not interval_bill_base or not interval_bill_solar or not interval_bill_final:
        st.info("Upload NEM12 data to run interval detailed billing with TOU/demand and dispatch battery.")
    else:
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Baseline annual bill", _currency(float(interval_bill_base["annual_bill"])))
        i2.metric("Solar-only annual bill", _currency(float(interval_bill_solar["annual_bill"])))
        i3.metric("Solar-only annual savings", _currency(float(interval_bill_base["annual_bill"] - interval_bill_solar["annual_bill"])))
        i4.metric("Final annual savings", _currency(float(interval_bill_base["annual_bill"] - interval_bill_final["annual_bill"])))

        j1, j2, j3, j4 = st.columns(4)
        j1.metric("Modelled PV generation", f"{interval_solar_meta.get('pv_generated_kwh', 0.0):,.0f} kWh")
        j2.metric("Modelled self-consumed PV", f"{interval_solar_meta.get('self_consumed_kwh', 0.0):,.0f} kWh")
        j3.metric("Modelled self-consumption", _pct(modelled_self_consumption_pct if modelled_self_consumption_pct is not None else 0.0))
        j4.metric("Solar load coverage", _pct(float(interval_solar_meta.get("solar_coverage_pct", 0.0))))

        if use_battery and battery_model_mode == "Dispatch (interval)":
            st.caption(
                f"Dispatch battery charged {interval_battery_meta.get('battery_charge_kwh', 0.0):,.1f} kWh, "
                f"discharged {interval_battery_meta.get('battery_discharge_kwh', 0.0):,.1f} kWh, "
                f"reduced imports by {interval_battery_meta.get('import_reduction_kwh', 0.0):,.1f} kWh over the loaded period."
            )

        bill_rows = []
        for name, b in [("Baseline", interval_bill_base), ("Solar-only", interval_bill_solar), ("Final", interval_bill_final)]:
            bill_rows.append(
                {
                    "Scenario": name,
                    "Usage $": float(b["usage_cost"]),
                    "Controlled $": float(b["controlled_cost"]),
                    "FiT credit $": float(b["fit_credit"]),
                    "Supply $": float(b["supply_cost"]),
                    "Demand $": float(b["demand_cost"]),
                    "Annual bill $": float(b["annual_bill"]),
                }
            )
        st.markdown("**Interval bill breakdown**")
        st.dataframe(pd.DataFrame(bill_rows), use_container_width=True)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Solar NPV (interval)", _currency(interval_solar_npv or 0.0))
        k2.metric("Solar ROI (interval)", _pct(interval_solar_roi_pct or 0.0))
        k3.metric("Solar IRR (interval)", _pct(interval_solar_irr_pct or 0.0) if interval_solar_irr_pct is not None else "-")
        k4.metric("Solar discounted payback (interval)", (f"{interval_solar_payback:.1f} years" if interval_solar_payback is not None else "-"))

for msg in interval_notes:
    st.caption(msg)

quoted_rows = []
if q:
    def _add_compare_row(
        label: str,
        quoted_val: Optional[float],
        calc_quote: float,
        calc_interval: Optional[float] = None,
        suffix: str = "",
    ) -> None:
        if quoted_val is None or (isinstance(quoted_val, float) and math.isnan(quoted_val)):
            return
        quoted = float(quoted_val)
        diff_quote = float(calc_quote) - quoted
        pct_quote = (diff_quote / quoted * 100.0) if quoted != 0 else float("nan")
        row = {
            "Metric": label,
            "Quoted": f"{quoted:,.2f}{suffix}",
            "Calc (quote-style)": f"{float(calc_quote):,.2f}{suffix}",
            "Diff quote-style": f"{diff_quote:,.2f}{suffix}",
            "Diff quote-style %": (f"{pct_quote:,.1f}%" if not math.isnan(pct_quote) else "-"),
        }
        if calc_interval is not None:
            diff_interval = float(calc_interval) - quoted
            pct_interval = (diff_interval / quoted * 100.0) if quoted != 0 else float("nan")
            row["Calc (interval detailed)"] = f"{float(calc_interval):,.2f}{suffix}"
            row["Diff interval"] = f"{diff_interval:,.2f}{suffix}"
            row["Diff interval %"] = (f"{pct_interval:,.1f}%" if not math.isnan(pct_interval) else "-")
        quoted_rows.append(row)

    _add_compare_row(
        "Annual bill before solar",
        q.get("quoted_annual_bill_before"),
        annual_bill_before_quoted_tariff,
        (interval_bill_base["annual_bill"] if interval_bill_base else None),
        " $",
    )
    _add_compare_row(
        "Annual bill with solar",
        q.get("quoted_annual_bill_after"),
        annual_bill_after_solar_quoted_tariff,
        (interval_bill_solar["annual_bill"] if interval_bill_solar else None),
        " $",
    )
    _add_compare_row("Annual savings", q.get("quoted_annual_savings"), annual_savings_solar_only_quoted_tariff, interval_annual_savings_solar, " $")
    _add_compare_row("20-year NPV", q.get("quoted_npv"), solar_npv_quoted_tariff, interval_solar_npv, " $")
    _add_compare_row("ROI", q.get("quoted_roi_pct"), solar_roi_pct_quoted_tariff, interval_solar_roi_pct, " %")
    _add_compare_row(
        "IRR",
        q.get("quoted_irr_pct"),
        (solar_irr_quoted_tariff * 100.0 if solar_irr_quoted_tariff is not None else float("nan")),
        interval_solar_irr_pct,
        " %",
    )

quote_compare_df = pd.DataFrame(quoted_rows) if quoted_rows else pd.DataFrame()
if not quote_compare_df.empty:
    st.subheader("6) Quote vs calculated check")
    st.dataframe(quote_compare_df, use_container_width=True)
    if q.get("quoted_discounted_payback_text"):
        interval_payback_text = (f"{interval_solar_payback:.1f}" if interval_solar_payback is not None else "-")
        st.caption(
            f"Quoted discounted payback: {q.get('quoted_discounted_payback_text')} years | "
            f"Calculated (quote-style): {(f'{solar_payback_discounted_quoted_tariff:.1f}' if solar_payback_discounted_quoted_tariff is not None else '-')} years | "
            f"Calculated (interval): {interval_payback_text} years"
        )

st.subheader("7) Restatement over time")
years = list(range(1, lifetime_years + 1))
savings_plot = {"Year": years, "Quote-style solar-only annual savings ($)": solar_yearly_savings_quoted_tariff}
if use_battery:
    savings_plot["Quote-style solar + battery annual savings ($)"] = final_yearly_savings_quoted_tariff
if interval_annual_savings_solar is not None:
    savings_plot["Interval solar-only annual savings ($)"] = interval_solar_yearly_savings
if use_battery and interval_annual_savings_final is not None:
    savings_plot["Interval solar + battery annual savings ($)"] = interval_final_yearly_savings

plot_df = pd.DataFrame(savings_plot).set_index("Year")


def _line_chart_full_legend(df_wide: pd.DataFrame, chart_title: str, y_axis_title: str) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        st.error("Matplotlib is required to render this chart.")
        st.dataframe(df_wide, use_container_width=True)
        return

    x_vals = pd.to_numeric(pd.Index(df_wide.index), errors="coerce")
    fig, ax = plt.subplots(figsize=(11, 4.8))
    for col in df_wide.columns:
        y_vals = pd.to_numeric(df_wide[col], errors="coerce")
        ax.plot(x_vals, y_vals, label=str(col), linewidth=2.0)

    ax.set_title(chart_title)
    ax.set_xlabel("Year")
    ax.set_ylabel(y_axis_title)
    ax.grid(True, alpha=0.3)
    ax.margins(x=0.02)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=1,
        frameon=False,
        fontsize=9,
    )
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)


_line_chart_full_legend(
    plot_df,
    chart_title="Annual Savings Over Time",
    y_axis_title="Annual savings ($/yr)",
)

cum_plot = {"Year": years}
for col in plot_df.columns:
    cum_plot[col.replace("annual", "cumulative")] = pd.Series(plot_df[col].values).cumsum().tolist()
cum_df = pd.DataFrame(cum_plot).set_index("Year")
_line_chart_full_legend(
    cum_df,
    chart_title="Cumulative Savings Over Time",
    y_axis_title="Cumulative savings ($)",
)

st.subheader("8) Export")
assumptions = {
    "Actual tariff source": actual_tariff_source,
    "Tariff mode": tariff_cfg.mode,
    "Actual import c/kWh (flat or TOU fallback)": tariff_cfg.flat_import_c_per_kwh,
    "Actual effective import c/kWh (used for quote-style)": plan.general_c_per_kwh,
    "Actual controlled c/kWh": tariff_cfg.controlled_c_per_kwh,
    "Actual FiT c/kWh": tariff_cfg.fit_c_per_kwh,
    "Actual supply $/day": tariff_cfg.supply_d_per_day,
    "Quoted flat import c/kWh": quoted_plan.general_c_per_kwh,
    "Quoted controlled c/kWh": quoted_plan.controlled_c_per_kwh,
    "Quoted FiT c/kWh": quoted_plan.feed_in_c_per_kwh,
    "Quoted supply $/day": quoted_plan.supply_d_per_day,
    "Invoice extracted import c/kWh": invoice_defaults.get("invoice_import_rate_c_per_kwh"),
    "Invoice extracted controlled c/kWh": invoice_defaults.get("invoice_controlled_rate_c_per_kwh"),
    "Invoice extracted FiT c/kWh": invoice_defaults.get("invoice_fit_rate_c_per_kwh"),
    "Invoice extracted supply $/day": invoice_defaults.get("invoice_supply_d_per_day"),
    "Invoice pages": invoice_defaults.get("invoice_pages"),
    "Invoice text chars": invoice_defaults.get("invoice_text_chars"),
    "Invoice text extract ok": invoice_defaults.get("invoice_text_extract_ok"),
    "Invoice OCR used": invoice_defaults.get("invoice_ocr_used"),
    "Invoice extract note": invoice_defaults.get("invoice_extract_note"),
    "Demand enabled": tariff_cfg.demand_enabled,
    "Demand c/kW/day": tariff_cfg.demand_c_per_kw_day,
    "Demand days": mode_to_day_label.get(tariff_cfg.demand_days, tariff_cfg.demand_days),
    "Demand window": f"{tariff_cfg.demand_start_hhmm}-{tariff_cfg.demand_end_hhmm}",
    "System size kWDC": system_size_kwdc,
    "Inverter size kWAC": ac_size_kw,
    "Annual production kWh": annual_production_kwh,
    "Self-consumption input %": self_consumption_pct,
    "Self-consumption used quote-style %": self_consumption_pct_quote,
    "Solar profile mode interval": solar_profile_mode,
    "Apply inverter cap interval": apply_inverter_cap,
    "Baseline source": baseline_source,
    "Battery enabled": use_battery,
    "Battery model mode": battery_model_mode,
    "Battery installed cost $": battery_cost if use_battery else 0.0,
    "Battery proxy capture %": battery_capture_pct if use_battery else 0.0,
    "Battery roundtrip eff %": battery_eff_pct if use_battery else 0.0,
    "Battery dispatch capacity kWh": battery_capacity_kwh if use_battery else 0.0,
    "Battery dispatch power kW": battery_power_kw if use_battery else 0.0,
    "Battery reserve SOC %": battery_reserve_pct if use_battery else 0.0,
    "Battery initial SOC %": battery_init_soc_pct if use_battery else 0.0,
    "Battery dispatch peak-only": battery_discharge_peak_only if use_battery else False,
}

results = {
    "Quote-style baseline bill $/yr": annual_bill_before,
    "Quote-style solar bill $/yr": annual_bill_after_solar,
    "Quote-style final bill $/yr": annual_bill_after_final,
    "Quote-style solar savings $/yr": annual_savings_solar_only,
    "Quote-style final savings $/yr": annual_savings_final,
    "Quoted-tariff baseline bill $/yr": annual_bill_before_quoted_tariff,
    "Quoted-tariff solar bill $/yr": annual_bill_after_solar_quoted_tariff,
    "Quoted-tariff final bill $/yr": annual_bill_after_final_quoted_tariff,
    "Quoted-tariff solar savings $/yr": annual_savings_solar_only_quoted_tariff,
    "Quoted-tariff final savings $/yr": annual_savings_final_quoted_tariff,
    "Tariff delta baseline bill $/yr (Actual - Quoted)": tariff_delta_baseline_bill,
    "Tariff delta solar bill $/yr (Actual - Quoted)": tariff_delta_solar_bill,
    "Tariff delta solar savings $/yr (Actual - Quoted)": tariff_delta_solar_savings,
    "Tariff delta final bill $/yr (Actual - Quoted)": tariff_delta_final_bill,
    "Tariff delta final savings $/yr (Actual - Quoted)": tariff_delta_final_savings,
    "Quote-style solar NPV $": solar_npv,
    "Quote-style solar ROI %": solar_roi_pct,
    "Quote-style solar IRR %": ((solar_irr or 0.0) * 100.0 if solar_irr is not None else None),
    "Quoted-tariff solar NPV $": solar_npv_quoted_tariff,
    "Quoted-tariff solar ROI %": solar_roi_pct_quoted_tariff,
    "Quoted-tariff solar IRR %": ((solar_irr_quoted_tariff or 0.0) * 100.0 if solar_irr_quoted_tariff is not None else None),
    "Interval baseline bill $/yr": (interval_bill_base["annual_bill"] if interval_bill_base else None),
    "Interval solar bill $/yr": (interval_bill_solar["annual_bill"] if interval_bill_solar else None),
    "Interval final bill $/yr": (interval_bill_final["annual_bill"] if interval_bill_final else None),
    "Interval solar savings $/yr": interval_annual_savings_solar,
    "Interval final savings $/yr": interval_annual_savings_final,
    "Interval modelled self-consumption %": modelled_self_consumption_pct,
}

report_md = build_report_markdown(
    run_title="Solar Quote Accuracy Checker Report",
    assumptions=assumptions,
    results=results,
    quote_comparison_df=quote_compare_df if not quote_compare_df.empty else None,
)
st.download_button("Download report (.md)", data=report_md, file_name="solar_quote_accuracy_report.md", mime="text/markdown")

pdf_bytes = markdown_to_pdf_bytes(report_md)
if pdf_bytes:
    st.download_button("Download report (.pdf)", data=pdf_bytes, file_name="solar_quote_accuracy_report.pdf", mime="application/pdf")
else:
    st.caption("PDF report export unavailable in this environment.")

run_payload = {
    "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    "assumptions": assumptions,
    "results": results,
    "quote_comparison": (quote_compare_df.to_dict(orient="records") if not quote_compare_df.empty else []),
    "interval_notes": interval_notes,
}
st.download_button(
    "Download run data (.json)",
    data=json.dumps(run_payload, indent=2, default=str),
    file_name="solar_quote_accuracy_run.json",
    mime="application/json",
)

st.markdown("**Model notes**")
st.caption(
    "This app now includes two paths: quote-style annual simplification and interval detailed simulation (flat or TOU, optional demand)."
)
st.caption(
    "Inverter size is optional for quote-style checks; for interval modelling it can be applied as an AC clipping cap to the generated PV profile."
)
st.caption(
    "Dispatch battery mode requires NEM12 data and uses interval charge/discharge constraints. Proxy battery mode remains available."
)

