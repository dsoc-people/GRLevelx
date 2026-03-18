"""
GRlevelX Placefile Dashboard — Streamlit app
Displays live KY Mesonet + WeatherStem surface observations and
lets you download the ready-to-use GRlevelX placefile.
"""

import gzip
import json
import math
import ssl
import urllib.request
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GRlevelX KY Surface Obs",
    page_icon="🌩️",
    layout="wide",
)

_SSL_CTX = ssl.create_default_context()

REFRESH_SECONDS = 300
THRESHOLD = 999

# ── Station lists ─────────────────────────────────────────────────────────────
MESONET_STATIONS = [
    ("FARM", 36.93, -86.47), ("RSVL", 36.85, -86.92), ("MRHD", 38.22, -83.48),
    ("MRRY", 36.61, -88.34), ("PCWN", 37.28, -84.96), ("HTFD", 37.45, -86.89),
    ("CMBA", 37.12, -85.31), ("CRMT", 37.94, -85.67), ("LXGN", 37.93, -84.53),
    ("BLRK", 37.47, -86.33), ("SCTV", 36.74, -86.21), ("PRNC", 37.09, -87.86),
    ("BMBL", 36.86, -83.84), ("PGHL", 36.94, -87.48), ("LSML", 38.08, -84.90),
    ("ERLN", 37.32, -87.49), ("OLIN", 37.36, -83.96), ("QKSD", 37.54, -83.32),
    ("SWON", 38.53, -84.77), ("LGNT", 37.54, -84.63), ("MROK", 36.95, -85.99),
    ("PVRT", 37.54, -87.28), ("BNGL", 37.36, -85.49), ("CRRL", 38.67, -85.15),
    ("HRDB", 37.77, -84.82), ("FRNY", 37.72, -87.90), ("GRDR", 36.79, -85.45),
    ("RPTN", 37.36, -88.07), ("ELST", 37.71, -84.18), ("DRFN", 36.88, -88.32),
    ("BTCK", 37.01, -88.96), ("WLBT", 37.83, -85.96), ("WSHT", 37.97, -82.50),
    ("WNCH", 38.01, -84.13), ("CCLA", 36.67, -88.67), ("BNVL", 37.28, -84.67),
    ("RNDH", 37.45, -82.99), ("HCKM", 36.85, -88.34), ("RBSN", 37.42, -83.02),
    ("HHTS", 36.96, -85.64), ("PRYB", 36.83, -83.17), ("CADZ", 36.83, -87.86),
    ("ALBN", 36.71, -85.14), ("HUEY", 38.97, -84.72), ("VEST", 37.41, -82.99),
    ("GRHM", 37.82, -87.51), ("CHTR", 38.58, -83.42), ("FLRK", 36.77, -84.48),
    ("DORT", 37.28, -82.52), ("FCHV", 38.16, -85.38), ("LGRN", 38.46, -85.47),
    ("HDYV", 37.26, -85.78), ("LUSA", 38.10, -82.60), ("PRST", 38.09, -83.76),
    ("BRND", 37.95, -86.22), ("LRTO", 37.63, -85.37), ("HDGV", 37.57, -85.70),
    ("WTBG", 37.13, -82.84), ("SWZR", 36.67, -86.61), ("CCTY", 37.29, -87.16),
    ("ZION", 36.76, -87.21), ("BMTN", 36.92, -82.91), ("WDBY", 37.18, -86.65),
    ("DANV", 37.62, -84.82), ("CROP", 38.33, -85.17), ("HARD", 37.76, -86.46),
    ("GAMA", 36.66, -85.80), ("DABN", 37.18, -84.56), ("DIXO", 37.52, -87.69),
    ("WADD", 38.09, -85.14), ("EWPK", 37.04, -86.35), ("RFVC", 37.46, -83.16),
    ("RFSM", 37.43, -83.18), ("CARL", 38.32, -84.04), ("MONT", 36.87, -84.90),
    ("BAND", 37.13, -88.95), ("WOOD", 36.99, -84.97), ("DCRD", 37.87, -83.65),
    ("SPIN", 38.13, -84.50), ("GRBG", 37.21, -85.47), ("PBDY", 37.14, -83.58),
    ("BLOM", 37.96, -85.31), ("LEWP", 37.92, -86.85), ("STAN", 37.85, -83.88),
    ("BEDD", 38.63, -85.32),
]

WEATHERSTEM_STATIONS = [
    ("WKU",               "WKU",                       36.9685,  -86.4708,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/warren/wku/latest.json"),
    ("WKUCHAOS",          "WKU Chaos",                 36.98583, -86.44967,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/warren/wkuchaos/latest.json"),
    ("WKUIMFIELDS",       "WKU IM Fields",             36.9742,  -86.4758,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/warren/wkuimfields/latest.json"),
    ("ETOWN",             "Elizabethtown",             37.6959,  -85.8789,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/hardin/wswelizabethtown/latest.json"),
    ("OWENSBORO",         "Owensboro",                 37.7719,  -87.1112,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/daviess/wswowensboro/latest.json"),
    ("GLASGOW",           "Glasgow",                   36.9959,  -85.9119,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/barren/wswglasgow/latest.json"),
    ("MAKERS_WAREHOUSE",  "Maker's Mark Warehouse",    37.6333,  -85.4076,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/marion-ky/makersmarkwarehouse/latest.json"),
    ("MAKERS_ST_MARY",    "Maker's Mark St Mary",      37.5708,  -85.3744,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/marion-ky/makersmarkstmary/latest.json"),
    ("MAKERS_LEBANON",    "Maker's Mark Lebanon",      37.5759,  -85.2737,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/marion-ky/makersmarklebanon/latest.json"),
    ("MAKERS_INNOVATION", "Maker's Mark Innovation",   37.6469,  -85.3490,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/marion-ky/makersmark/latest.json"),
    ("JB_BOOKER_NOE",     "Jim Beam Booker Noe",       37.8128,  -85.6849,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/nelson/jbbookernoe/latest.json"),
    ("JB_BARDSTOWN",      "Jim Beam Bardstown",        37.8345,  -85.4711,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/nelson/jbbardstown/latest.json"),
    ("JB_CLERMONT",       "Jim Beam Clermont",         37.9318,  -85.6520,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/bullitt/jbclermont/latest.json"),
    ("JB_OLD_CROW",       "Jim Beam Old Crow",         38.1464,  -84.8415,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/franklin-ky/jboldcrow/latest.json"),
    ("JB_GRAND_DAD",      "Jim Beam Grand Dad",        38.2157,  -84.8093,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/franklin-ky/jbgranddad/latest.json"),
    ("WOODFORD_CH",       "Woodford Co. Courthouse",   38.0527,  -84.7307,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/woodford/courthouse/latest.json"),
    ("ADAIR_HS",          "Adair Co. High School",     37.1077,  -85.3282,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/adair/achs/latest.json"),
    ("CLINTON_HS",        "Clinton Co. High School",   36.7082,  -85.1313,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/clinton/clintonhs/latest.json"),
    ("NOVELIS_GUTHRIE",   "Novelis Guthrie",           36.6025,  -87.7186,
     "https://cdn.weatherstem.com/dashboard/data/dynamic/model/todd/novelis/latest.json"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 10):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "GRlevelX-Dashboard/1.0", "Accept-Encoding": "gzip"},
        )
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
            raw = r.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def c_to_f(c):
    try:
        return round(float(c) * 9 / 5 + 32)
    except (TypeError, ValueError):
        return None


def mps_to_mph(mps):
    try:
        return round(float(mps) * 2.23694)
    except (TypeError, ValueError):
        return None


def deg_to_cardinal(deg):
    if deg is None:
        return "---"
    try:
        deg = float(deg) % 360
    except (TypeError, ValueError):
        return "---"
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[round(deg / 22.5) % 16]


# ── Fetch functions ───────────────────────────────────────────────────────────

def fetch_mesonet(station_id, lat, lon):
    year = datetime.now(timezone.utc).year
    for yr in (year, year - 1):
        manifest = fetch_json(
            f"https://d266k7wxhw6o23.cloudfront.net/data/{station_id}/{yr}/manifest.json"
        )
        if manifest:
            break
    else:
        return None

    days = sorted(manifest.keys())
    if not days:
        return None
    key = manifest[days[-1]]["key"]
    data = fetch_json(f"https://d266k7wxhw6o23.cloudfront.net/{key}")
    if not data:
        return None

    rows, cols = data.get("rows", []), data.get("columns", [])
    if not rows or not cols:
        return None

    def col(name):
        try:
            return rows[-1][cols.index(name)]
        except (ValueError, IndexError):
            return None

    return {
        "id": station_id, "name": station_id, "lat": lat, "lon": lon,
        "temp": c_to_f(col("TAIR")), "dewpoint": c_to_f(col("DWPT")),
        "wind_spd": mps_to_mph(col("WSPD")), "wind_dir": col("WDIR"),
        "source": "KY Mesonet",
    }


def fetch_weatherstem(station_id, name, lat, lon, url):
    data = fetch_json(url)
    if not data:
        return None
    records = data.get("records", [])

    def sensor(keyword):
        kw = keyword.lower()
        for r in records:
            if kw in (r.get("sensor_name") or "").lower():
                try:
                    return float(r.get("value"))
                except (TypeError, ValueError):
                    return None
        return None

    raw_t = sensor("thermometer")
    raw_d = sensor("dewpoint")
    raw_w = sensor("anemometer")
    raw_v = sensor("wind vane")
    return {
        "id": station_id, "name": name, "lat": lat, "lon": lon,
        "temp": round(raw_t) if raw_t is not None else None,
        "dewpoint": round(raw_d) if raw_d is not None else None,
        "wind_spd": round(raw_w) if raw_w is not None else None,
        "wind_dir": round(raw_v) if raw_v is not None else None,
        "source": "WeatherStem",
    }


# ── Cached data loader (5-minute TTL) ─────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_stations():
    mesonet = [fetch_mesonet(sid, lat, lon) for sid, lat, lon in MESONET_STATIONS]
    weatherstem = [fetch_weatherstem(sid, n, lat, lon, u)
                   for sid, n, lat, lon, u in WEATHERSTEM_STATIONS]
    return [s for s in mesonet + weatherstem if s is not None]


# ── Placefile builder ─────────────────────────────────────────────────────────

def build_placefile(stations):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    lines = [
        f"; KY Surface Observations - KY Mesonet + WeatherStem",
        f"; Generated: {now_utc}",
        "",
        f"Title: KY Surface Obs ({now_utc})",
        f"RefreshSeconds: {REFRESH_SECONDS}",
        f"Threshold: {THRESHOLD}",
        "",
        'Font: 1, 11, 1, "Courier New"',
        'Font: 2, 10, 0, "Courier New"',
        "",
    ]
    for s in stations:
        temp  = str(s["temp"])  if s["temp"]     is not None else "N/A"
        dew   = str(s["dewpoint"]) if s["dewpoint"] is not None else "N/A"
        wspd  = f"{s['wind_spd']} mph" if s["wind_spd"] is not None else "N/A"
        card  = deg_to_cardinal(s["wind_dir"])
        wdeg  = f"{round(s['wind_dir'])}deg" if s["wind_dir"] is not None else "N/A"
        hover = (f"{s['name']} [{s['source']}]\\n"
                 f"Temp: {temp}F  Dew: {dew}F\\n"
                 f"Wind: {wspd} from {card} ({wdeg})")
        id6 = s["id"][:6]
        lines += [
            f"Object: {s['lat']},{s['lon']}",
            f'  Text: 0, 14,1,"{temp}","{hover}"',
            f'  Text: 0,-14,2,"{dew}","{hover}"',
            f'  Text: 16,  0,2,"{id6}","{hover}"',
            "End:", "",
        ]
    return "\n".join(lines)


# ── Streamlit UI ──────────────────────────────────────────────────────────────

st.title("KY Surface Observations")
st.caption("KY Mesonet + WeatherStem | GRlevelX Placefile Dashboard")

# Header row: last-updated + refresh button
col_ts, col_btn = st.columns([4, 1])
with col_ts:
    now_display = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    st.markdown(f"**Last updated:** {now_display}  *(auto-refreshes every 5 min)*")
with col_btn:
    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()

# Load data
with st.spinner("Fetching station data..."):
    stations = load_stations()

if not stations:
    st.error("Could not fetch any station data. Check network connectivity.")
    st.stop()

ok_count = len(stations)
st.success(f"Loaded **{ok_count}** stations ({sum(1 for s in stations if s['source']=='KY Mesonet')} KY Mesonet, "
           f"{sum(1 for s in stations if s['source']=='WeatherStem')} WeatherStem)")

# ── Map ───────────────────────────────────────────────────────────────────────
st.subheader("Station Map")

df_map = pd.DataFrame([
    {
        "lat": s["lat"],
        "lon": s["lon"],
        "id": s["id"],
        "name": s["name"],
        "temp": s["temp"] if s["temp"] is not None else float("nan"),
        "dewpoint": s["dewpoint"] if s["dewpoint"] is not None else float("nan"),
        "wind_spd": s["wind_spd"] if s["wind_spd"] is not None else float("nan"),
        "wind_dir": deg_to_cardinal(s["wind_dir"]),
        "source": s["source"],
    }
    for s in stations
])

st.map(df_map, latitude="lat", longitude="lon", size=50)

# ── Data table ────────────────────────────────────────────────────────────────
st.subheader("Station Data")

df_table = df_map[["id", "name", "source", "temp", "dewpoint", "wind_spd", "wind_dir"]].copy()
df_table.columns = ["ID", "Name", "Source", "Temp (F)", "Dewpt (F)", "Wind (mph)", "Wind Dir"]
df_table = df_table.sort_values("ID").reset_index(drop=True)

st.dataframe(df_table, use_container_width=True, hide_index=True)

# ── Placefile download ────────────────────────────────────────────────────────
st.subheader("GRlevelX Placefile")
st.markdown(
    "Download the placefile and load it in GRlevelX via **GIS menu → Placefile Manager → Add**."
)

placefile_text = build_placefile(stations)

st.download_button(
    label="Download ky_obs.txt",
    data=placefile_text.encode("utf-8"),
    file_name="ky_obs.txt",
    mime="text/plain",
)

with st.expander("Preview placefile (first 60 lines)"):
    preview = "\n".join(placefile_text.splitlines()[:60])
    st.code(preview, language="text")
