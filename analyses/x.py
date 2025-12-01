from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text


PROJECTS = [
    "7571","7796","7797","7798","7800","7803","7804","7809","7812","7813","7814","7818","7819","7825","7828","7833",
    "7951-001","7951-003","7951-050","7951-051","7951-054","7951-057","7951-062","7951-063","7951-065",
    "7951-067","7951-071","7951-079","7951-081","7951-083","7951-085","7951-086","7951-087","7951-088",
    "7951-091","7951-093","7951-094","7951-096","7951-099","7951-100","7951-108","7951-112","7951-114",
    "7951-115","7951-118","7951-121","7951-122","7951-124","7951-125","7951-128","7951-130","7951-131",
    "7951-134","7951-135","7951-139","7951-142","7951-149",
    "8266-156","8266-160","8266-161","8266-163","8266-165","8266-166","8266-167","8266-168","8266-174",
    "8266-179","8266-184","8266-185","8266-187","8266-191","8266-196","8266-197","8266-199","8266-203",
    "8266-208","8266-209","8266-210","8266-211","8266-214","8266-217","8266-218","8266-221","8266-222",
    "8266-223","8266-227","8266-230","8266-234","8266-240","8266-246","8266-247","8266-250","8266-254",
    "8266-259","8266-266","8266-269","8266-272","8266-273","8266-274",
    "8558-276","8558-281","8558-282","8558-283","8558-289","8558-292","8558-301","8558-304","8558-311",
    "8558-313","8558-314","8558-317","8558-318","8558-320","8558-321","8558-322","8558-324","8558-328",
    "8558-330","8558-336","8558-337","8558-339","8558-340",
]

SITE_MAP = {
    "7571": "Orignolles",
    "7796": "Meru",
    "7797": "Charleval",
    "7798": "Triel",
    "7800": "Saujon",
    "7803": "Cierzac",
    "7804": "Os Marsillon",
    "7809": "St Pere en retz",
    "7812": "Hagetmau",
    "7813": "Biscarosse",
    "7814": "Auriolles",
    "7818": "Verneuil",
    "7819": "Allaire",
    "7825": "Vezin",
    "7828": "Pontchateau",
    "7833": "Pontfaverger",
    "001": "Baud", "003": "Maurs", "050": "Mezidon", "051": "Derval", "054": "Campagne", "057": "Mailly le Chateau", "062": "Winnezeele", "063": "Diges", "065": "Vernouillet", "067": "Orbec", "071": "St Renan", "079": "Molompize", "081": "Carquefou", "083": "Vaupillon", "085": "Pleumartin", "086": "Caumont sur Aure", "087": "Getigne", "088": "Chinon", "091": "La Roche sur Yon", "093": "Aubigne sur Layon", "094": "Bonvillet", "096": "Rambervillers", "099": "Blere", "100": "Plouasne", "108": "Champniers", "112": "Nissan Lez Enserune", "114": "Combourg", "115": "Vimoutiers", "118": "Beaumont de Lomagne", "121": "Sueves", "122": "Maen Roch", "124": "St Leon sur L Isle", "125": "Mirecourt", "128": "La Voge les Bains", "130": "Amanvillers", "131": "Guerlesquin", "134": "Guerande", "135": "Riscle", "139": "Avrille", "142": "Domfront", "149": "Couesmes", "156": "Ste Catherine", "160": "Andel", "161": "Chazey Bons", "163": "Lauzerte", "165": "Trie la ville", "166": "Hambach", "167": "Beaugency", "168": "Carcassonne", "174": "Sable sur Sarthe", "179": "Taden", "184": "Rue", "185": "Quevilloncourt", "187": "St Victor de Morestel", "191": "St Hilaire du Harcouet", "196": "Hémonstoir", "197": "Amily", "199": "Henrichemont", "203": "Couleuvre", "208": "St Pierre le Moutier 2", "209": "Bourbon L Archambaut", "210": "Brou", "211": "Neulise", "214": "St Jean le vieux", "217": "Periers", "218": "Quievrecourt", "221": "Chazelle sur Lyon", "222": "Montverdun", "223": "Dormans", "227": "Glonville 2", "230": "Montalieu Vercieu", "234": "Nesle Normandeuse", "240": "Noyal Pontivy", "246": "Vitre 2", "247": "St Amour", "250": "Dourdan", "254": "Roanne", "259": "Plufur", "266": "Boinville en Mantois", "269": "Loche", "272": "Bonnieres sur Seine", "273": "Piffonds", "274": "St Benin d Azy", "276": "Niort St Florent", "281": "Chauffailles", "282": "St Vincent d Autejac", "283": "Culhat", "289": "Loireauxence", "292": "Reuil", "301": "Coteaux sur Loire", "304": "Le Mans 2", "311": "Chantrigne", "313": "St Thelo", "314": "St Pierre la cour", "317": "Nievroz", "318": "Val Revermont", "320": "Mondoubleau", "321": "Kernoues", "322": "Yvetot Bocage", "324": "Douchy Montcorbon", "328": "Sully sur Loire B", "330": "Vincey", "336": "Ville en Vermois", "337": "Virandeville", "339": "Reims", "340": "Reims B", "342": "Charge", "343": "St Benoit la Foret", "349": "Dombrot le Sec", "352": "Riorges", "362": "Montauban B", "365": "Dogneville 2", "366": "Brieulles sur meuse", "368": "Melesse", "372": "Pujaudran", "374": "Plouye", "376": "Dampierre en Burly", "381": "Dommartin les Remiremont", "382": "St Igny de Roche", "384": "Guengat", "386": "Epeigne sur deme 2", "388": "Maiche", "391": "Wittenheim", "394": "Lacres", "395": "Trelivan", "397": "Vironvay", "399": "Abbeville les Conflans", "401": "Orgeval", "402": "Mantes la Ville", "403": "Liny devant Dun B", "412": "St Leger sur Roanne", "414": "Mairy Mainville",
}

DB_CONFIG_CHARGE = {
    "host": "162.19.251.55",
    "port": 3306,
    "user": "nidec",
    "password": "MaV38f5xsGQp83",
    "database": "Charges",
}

def env(name: str, default: str) -> str:
    """Return the value of an environment variable with a default fallback."""
    return os.environ.get(name, default)


INFLUX_HOST = env("INFLUX_HOST", "tsdbe.nidec-asi-online.com")
INFLUX_PORT = env("INFLUX_PORT", "443")
INFLUX_USER = env("INFLUX_USER", "nw")
INFLUX_PW = env("INFLUX_PW", "at3Dd94Yp8BT4Sh!")
INFLUX_DB = env("INFLUX_DB", "signals")
INFLUX_MEAS = env("INFLUX_MEAS", "fastcharge")
INFLUX_TAG_PROJECT = env("INFLUX_TAG_PROJECT", "project")


SIGNAL_MAP = {
    1: "EVI_P1.ILI.EVSE_OutVoltage",
    2: "EVI_P2.ILI.EVSE_OutVoltage",
    3: "EVI_P3.ILI.EVSE_OutVoltage",
    4: "EVI_P4.ILI.EVSE_OutVoltage",
}


ERROR_CODE = 84
ERROR_STEP = 7
NUM_WORKERS = 10  # Number of parallel workers for processing charges

# Heuristics used to qualify the signal behaviour.
FLAT_ABS_TOLERANCE = 1e-6
PEAK_THRESHOLD = 100.0
BETWEEN_LOW = 30.0
BETWEEN_HIGH = 70.0

# Lecture EVI detection threshold (in volts)
# "Lecture EVI" occurs when signal is near 0V (no active charging)
LECTURE_EVI_MAX = 10.0  # Maximum voltage for "Lecture EVI" classification


class InfluxClient:
    """Minimal HTTP client for InfluxDB 1.x queries."""

    def __init__(self) -> None:
        self._base_url = f"https://{INFLUX_HOST}:{INFLUX_PORT}/query"
        self._auth = (INFLUX_USER, INFLUX_PW)

    def query(self, query: str) -> pd.DataFrame:
        params = {"db": INFLUX_DB, "q": query, "epoch": "s"}
        response = requests.get(self._base_url, params=params, auth=self._auth, timeout=30)
        response.raise_for_status()
        payload = response.json()

        results = payload.get("results", [])
        if not results:
            return pd.DataFrame(columns=["time", "value"])

        series = results[0].get("series")
        if not series:
            return pd.DataFrame(columns=["time", "value"])

        values = series[0].get("values", [])
        if not values:
            return pd.DataFrame(columns=["time", "value"])

        return pd.DataFrame(values, columns=series[0].get("columns", ["time", "value"]))


def _build_engine(config: dict):
    """Build a SQLAlchemy engine from the existing configuration."""
    return create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**config)
    )


def parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iter_project_candidates(row: Mapping[str, Any]) -> Iterator[str]:
    """Generate all possible project name variants to try in InfluxDB.
    
    InfluxDB uses project IDs from PROJECTS list which have formats like:
    - "7571", "7796" (direct numbers)
    - "7951-001", "8266-160", "8558-317" (prefix-number format)
    
    We need to find the correct format from site names in MySQL.
    """
    
    def remove_accents(text: str) -> str:
        import unicodedata
        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    
    def normalize(text: str) -> str:
        """Normalize text for comparison: lowercase and no accents."""
        return remove_accents(text).lower().strip()
    
    seen = set()
    
    # Get base values
    site = str(row.get("Site", "")).strip()
    name_project = str(row.get("Name Project", "")).strip()
    
    if not site and not name_project:
        return
    
    # PRIORITY 1: Find exact match in PROJECTS list
    # First, do reverse lookup in SITE_MAP to get the numeric ID
    for search_name in [site, name_project]:
        if not search_name:
            continue
            
        search_normalized = normalize(search_name)
        
        for site_id, site_name in SITE_MAP.items():
            site_name_normalized = normalize(site_name)
            
            # Check if names match
            if search_normalized == site_name_normalized:
                # Found the numeric ID! Now search for it in PROJECTS
                # Try to find entries in PROJECTS that end with this ID
                for project in PROJECTS:
                    # Check if project matches exactly or ends with "-{site_id}"
                    if project == site_id or project.endswith(f"-{site_id}"):
                        if project not in seen:
                            seen.add(project)
                            yield project
    
    # PRIORITY 2: Try direct numeric IDs from SITE_MAP
    for search_name in [site, name_project]:
        if not search_name:
            continue
            
        search_normalized = normalize(search_name)
        
        for site_id, site_name in SITE_MAP.items():
            site_name_normalized = normalize(site_name)
            
            if search_normalized == site_name_normalized:
                if site_id not in seen:
                    seen.add(site_id)
                    yield site_id
                
                # Try with leading zeros
                for padding in [3, 4]:
                    padded = site_id.zfill(padding)
                    if padded not in seen:
                        seen.add(padded)
                        yield padded
    
    # PRIORITY 3: Try the original names as fallback
    if site and site not in seen:
        seen.add(site)
        yield site
        
    if name_project and name_project != site and name_project not in seen:
        seen.add(name_project)
        yield name_project


def _get_table_columns(engine, schema: str, table: str) -> set[str]:
    """Return the set of columns available on a given table."""
    query = text(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME = :table
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query, {"schema": schema, "table": table})
        return {row[0] for row in result}


def fetch_charges(engine, start: Optional[datetime], end: Optional[datetime]) -> pd.DataFrame:
    """Retrieve all charges matching the target error."""

    start_clause = ""
    if start:
        start_clause = " AND `Datetime start` >= '{date}'".format(
            date=start.strftime("%Y-%m-%d")
        )

    end_clause = ""
    if end:
        end_clause = " AND `Datetime start` < '{date}'".format(
            date=(end + timedelta(days=1)).strftime("%Y-%m-%d")
        )

    available_columns = _get_table_columns(engine, DB_CONFIG_CHARGE["database"], "kpi_sessions")

    select_columns: list[str] = []
    column_order = [
        "Site",
        "Name Project",
        "PDC",
        "Datetime start",
        "Datetime end",
        "EVI Error Code",
        "EVI Status during error",
        "ID",
        "SOC Start",
        "SOC End",
        "Energy (Kwh)",
    ]

    for column in column_order:
        if column in available_columns:
            select_columns.append(f"`{column}`")

    optional_columns = []
    if "Id Project" in available_columns:
        optional_columns.append("`Id Project`")

    select_clause = ",\n            ".join(select_columns + optional_columns)

    if not select_clause:
        select_clause = "1"

    order_by_column = "`Datetime start`" if "Datetime start" in available_columns else None
    order_by_clause = ""
    if order_by_column:
        order_by_clause = f"\n        ORDER BY {order_by_column} ASC"

    query = f"""
        SELECT
            {select_clause}
        FROM Charges.kpi_sessions
        WHERE `EVI Error Code` = {ERROR_CODE}
          AND `EVI Status during error` = {ERROR_STEP}
          {start_clause}
          {end_clause}
        {order_by_clause}
    """

    df = pd.read_sql(query, con=engine)

    for col in ("Datetime start", "Datetime end"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = pd.NaT

    df["PDC"] = pd.to_numeric(df.get("PDC"), errors="coerce").astype("Int64")

    if "Site" not in df.columns:
        df["Site"] = df.get("Name Project", "")

    if "Name Project" not in df.columns:
        df["Name Project"] = df.get("Site", "")

    if "Id Project" not in df.columns:
        df["Id Project"] = pd.NA

    missing_site = df["Site"].astype(str).str.strip().eq("")
    if missing_site.any() and df["Id Project"].notna().any():
        candidates = df.loc[missing_site, "Id Project"].apply(
            lambda value: str(value).strip() if pd.notna(value) else ""
        )
        mapped_sites = candidates.map(SITE_MAP).fillna(candidates)
        df.loc[missing_site, "Site"] = mapped_sites

    still_missing = df["Site"].astype(str).str.strip().eq("")
    if still_missing.any():
        df.loc[still_missing, "Site"] = df.loc[still_missing, "Name Project"].fillna("")

    return df


def describe_signal(values: pd.Series) -> str:
    """Classify voltage signal patterns according to the image specifications.
    
    Rules:
    1. Flat signal with very small variations (stable voltage) → "Lecture EVI"
    2. Signal with TWO distinct peaks separated by a significant drop → "Réglage Variateur"
    3. Other patterns (plateaus, single peaks, etc.) → "Autre"
    """
    if values.empty:
        return "Aucune donnée Influx"

    arr = values.astype(float).to_numpy()
    
    if len(arr) == 0:
        return "Aucune donnée Influx"
    
    min_voltage = np.min(arr)
    max_voltage = np.max(arr)
    mean_voltage = np.mean(arr)
    voltage_range = max_voltage - min_voltage
    
    # Rule 1: Flat signal near ZERO (Lecture EVI)
    # "Lecture EVI" = no active charging, signal near 0V (0 ± 10V max)
    
    if max_voltage <= LECTURE_EVI_MAX:
        return "Lecture EVI"
    
    # Rule 2: Detect "Réglage Variateur" pattern
    # This is characterized by TWO distinct peaks with a significant drop between them
    # Strategy: find local maxima that are above threshold, check if we have 2+ peaks
    # with meaningful valleys between them
    
    peaks = []
    valleys = []
    
    # Find local extrema in the signal
    for i in range(1, len(arr) - 1):
        # Peak: higher than neighbors
        if arr[i] > arr[i-1] and arr[i] > arr[i+1]:
            if arr[i] >= PEAK_THRESHOLD:  # Only consider peaks above 100V
                peaks.append((i, arr[i]))
        # Valley: lower than neighbors
        elif arr[i] < arr[i-1] and arr[i] < arr[i+1]:
            if arr[i] < BETWEEN_HIGH:  # Valley should be relatively low
                valleys.append((i, arr[i]))
    
    # Handle edge cases - check if start/end are peaks
    if len(arr) > 1:
        if arr[0] > arr[1] and arr[0] >= PEAK_THRESHOLD:
            if not peaks or peaks[0][0] != 0:
                peaks.insert(0, (0, arr[0]))
        if arr[-1] > arr[-2] and arr[-1] >= PEAK_THRESHOLD:
            if not peaks or peaks[-1][0] != len(arr) - 1:
                peaks.append((len(arr) - 1, arr[-1]))
    
    # Check if we have at least 2 peaks with a significant drop between them
    if len(peaks) >= 2:
        first_peak_idx, first_peak_val = peaks[0]
        second_peak_idx, second_peak_val = peaks[1]
        
        # Make sure peaks are in order and have space between them
        if first_peak_idx < second_peak_idx:
            # Check what's between the two peaks
            between_start = first_peak_idx + 1
            between_end = second_peak_idx
            
            if between_end > between_start:
                between_values = arr[between_start:between_end]
                if len(between_values) > 0:
                    min_between = np.min(between_values)
                    max_peak = max(first_peak_val, second_peak_val)
                    
                    # Check for significant drop
                    # Drop should be at least 40% from the peak value
                    # (i.e., valley should be <= 60% of peak)
                    drop_percentage = ((max_peak - min_between) / max_peak) * 100
                    
                    if drop_percentage >= 40:  # At least 40% drop
                        return "Réglage Variateur"
    
    # Rule 3: Everything else (single peaks, plateaus, etc.)
    return "Autre"


def load_signal(
    client: InfluxClient,
    field: str,
    start: datetime,
    end: datetime,
    project_candidates: Iterable[str],
) -> pd.DataFrame:
    """Load signal from InfluxDB with extended time window.
    
    Extends the time window by 5 seconds before and after to capture
    the full signal pattern for better analysis.
    """
    # Extend time window by 5 seconds before and after
    utc_start = ensure_utc(start) - timedelta(seconds=5)
    utc_end = ensure_utc(end) + timedelta(seconds=5)
    
    base_conditions = f"time >= '{utc_start.isoformat()}' AND time <= '{utc_end.isoformat()}'"

    for candidate in project_candidates:
        # Escape single quotes in the candidate name for InfluxDB
        escaped_candidate = candidate.replace("'", "\\'")
        tag_condition = f'"{INFLUX_TAG_PROJECT}" = \'{escaped_candidate}\''
        query = f'SELECT "{field}" FROM "{INFLUX_MEAS}" WHERE {tag_condition} AND {base_conditions}'
        
        try:
            result = client.query(query)
            if not result.empty:
                return result
        except Exception:
            # Silently continue to next candidate
            continue
    
    return pd.DataFrame(columns=["time", field])


def _normalize_interval(row: Mapping[str, Any]) -> Optional[tuple[datetime, datetime]]:
    """Return a (start, end) tuple ensuring we have a sensible time range."""
    start = row.get("Datetime start")
    end = row.get("Datetime end")

    if pd.isna(start):
        return None

    if pd.isna(end) or end <= start:
        end = start + timedelta(hours=1)

    return start.to_pydatetime() if isinstance(start, pd.Timestamp) else start, (
        end.to_pydatetime() if isinstance(end, pd.Timestamp) else end
    )


def _process_charge(row: pd.Series) -> Optional[dict[str, Any]]:
    """Process a single charge and return its record."""
    interval = _normalize_interval(row)
    if interval is None:
        return None

    pdc = row.get("PDC")
    if pd.isna(pdc):
        return None

    pdc_int = int(pdc)
    field = SIGNAL_MAP.get(pdc_int)
    if not field:
        return None

    start, end = interval
    project_candidates = list(dict.fromkeys(iter_project_candidates(row)))
    client = InfluxClient()
    signal_df = load_signal(client, field, start, end, project_candidates)

    if signal_df.empty:
        comment = "Aucune donnée Influx"
        min_val = np.nan
        max_val = np.nan
    else:
        value_col = field if field in signal_df.columns else signal_df.columns[-1]
        series = pd.to_numeric(signal_df[value_col], errors="coerce").dropna()
        min_val = series.min() if not series.empty else np.nan
        max_val = series.max() if not series.empty else np.nan
        comment = describe_signal(series)

    return {
        "Site": row.get("Site") or row.get("Name Project"),
        "Name Project": row.get("Name Project"),
        "Id Project": row.get("Id Project"),
        "PDC": pdc_int,
        "ID": row.get("ID"),
        "SOC Start": row.get("SOC Start"),
        "SOC End": row.get("SOC End"),
        "Energy (Kwh)": row.get("Energy (Kwh)"),
        "Datetime start": row.get("Datetime start"),
        "Datetime end": row.get("Datetime end"),
        "Signal": field,
        "Min Voltage": min_val,
        "Max Voltage": max_val,
        "Commentaire": comment,
    }


def build_report(df: pd.DataFrame, output: Path) -> Path:
    records: List[dict[str, Any]] = []
    total = len(df)
    
    print(f"Traitement de {total} charges avec {NUM_WORKERS} workers...")
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(_process_charge, row): idx for idx, (_, row) in enumerate(df.iterrows())}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"Progression: {completed}/{total} charges traitées")
            
            try:
                record = future.result()
                if record:
                    records.append(record)
            except Exception as exc:
                # Log error but continue processing
                idx = futures[future]
                print(f"Erreur lors du traitement de la charge {idx}: {exc}")

    if not records:
        report_df = pd.DataFrame(
            columns=[
                "Site",
                "Name Project",
                "Id Project",
                "PDC",
                "ID",
                "SOC Start",
                "SOC End",
                "Energy (Kwh)",
                "Datetime start",
                "Datetime end",
                "Signal",
                "Min Voltage",
                "Max Voltage",
                "Commentaire",
            ]
        )
    else:
        report_df = pd.DataFrame(records)

    report_df.sort_values(["Site", "Datetime start", "PDC"], inplace=True)

    output.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_excel(output, index=False)
    return output


def main(argv: Optional[Sequence[str]] = None) -> Path:
    parser = argparse.ArgumentParser(description="Analyse des tensions EVI pour l'erreur 84 / moment 7")
    parser.add_argument("--start", type=parse_date, help="Date de début (YYYY-MM-DD), par défaut: 2025-06-02")
    parser.add_argument("--end", type=parse_date, help="Date de fin (YYYY-MM-DD), par défaut: aujourd'hui")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("exports/evi_voltage_report.xlsx"),
        help="Chemin du fichier Excel de sortie",
    )

    args = parser.parse_args(argv)
    
    # Set default start date to 2025-06-02 if not provided
    if args.start is None:
        args.start = datetime(2025, 6, 2)
        print(f"Date de début par défaut: {args.start.strftime('%Y-%m-%d')}")
    
    # Set default end date to today if not provided
    if args.end is None:
        args.end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Date de fin par défaut: {args.end.strftime('%Y-%m-%d')}")

    engine = _build_engine(DB_CONFIG_CHARGE)
    charges = fetch_charges(engine, args.start, args.end)

    if charges.empty:
        print("Aucune charge avec l'erreur EVI 84 / step 7 trouvée dans l'intervalle spécifié.")
        return args.output

    print(f"{len(charges)} charges trouvées.")
    output_path = build_report(charges, args.output)
    print(f"\nRapport généré : {output_path}")
    return output_path


if __name__ == "__main__":
    main()