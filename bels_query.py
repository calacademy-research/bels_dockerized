__author__ = "John Wieczorek"
__copyright__ = "Copyright 2022 Rauthiflor LLC"
__filename__ = "bels_query.py"
__version__ = __filename__ + ' ' + "2022-06-20T13:25-03:00"

import os
import json
import logging

import psycopg2
import psycopg2.extras

from api.config import Config
from json_utils import CustomJsonEncoder
from dwca_utils import lower_dict_keys

# Schemas in the PostgreSQL database—make sure these exist (or adjust as needed)
PG_SCHEMA_SERVICE = 'localityservice'
PG_SCHEMA_GAZ = 'gazetteer'
PG_SCHEMA_VOCABS = 'vocabs'
PG_SCHEMA_INPUT = 'belsapi'
PG_SCHEMA_OUTPUT = 'results'


def get_pg_connection():
    """Establish a connection to PostgreSQL using settings in Config."""
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD
    )


def run_pg_query(conn, querystr, max_results=None):
    """
    Execute a SQL query against Postgres.
    Returns a list of dicts (via DictCursor). If max_results is set, fetch that many.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(querystr)
        if max_results:
            return cur.fetchmany(max_results)
        return cur.fetchall()


def georeference_score(locdict):
    if locdict is None:
        return None
    lowerlocdict = lower_dict_keys(locdict)
    score = 0
    if lowerlocdict.get('georeferenceprotocol'):
        score += 16
    if lowerlocdict.get('georeferencesources'):
        score += 8
    if lowerlocdict.get('georeferenceddate'):
        score += 4
    if lowerlocdict.get('georeferencedby'):
        score += 2
    if lowerlocdict.get('georeferenceremarks'):
        score += 1
    return score


def coordinates_score(locdict):
    if locdict is None:
        return None
    lowerlocdict = lower_dict_keys(locdict)
    score = 0
    try:
        lat = float(lowerlocdict.get('decimallatitude'))
        lng = float(lowerlocdict.get('decimallongitude'))
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            score += 128
    except Exception:
        pass
    if lowerlocdict.get('geodeticdatum'):
        score += 64
    try:
        unc = float(lowerlocdict.get('coordinateuncertaintyinmeters'))
        if 1 <= unc <= 20037509:
            score += 32
    except Exception:
        pass
    score += georeference_score(locdict) or 0
    return score


def has_georef(locdict):
    if locdict is None:
        return None
    return coordinates_score(locdict) >= 224


def has_decimal_coords(locdict):
    if locdict is None:
        return None
    return coordinates_score(locdict) >= 128


def has_verbatim_coords(locdict):
    if locdict is None:
        return None
    return bool(
        locdict.get('verbatimlatitude') and locdict.get('verbatimlongitude')
        or locdict.get('verbatimcoordinates')
    )


def bels_original_georef(locdict):
    if locdict is None:
        return None
    return {
        'bels_countrycode': None,
        'bels_match_string': None,
        'bels_decimallatitude': locdict.get('decimallatitude'),
        'bels_decimallongitude': locdict.get('decimallongitude'),
        'bels_geodeticdatum': locdict.get('geodeticdatum'),
        'bels_coordinateuncertaintyinmeters': locdict.get('coordinateuncertaintyinmeters'),
        'bels_georeferencedby': locdict.get('georeferencedby'),
        'bels_georeferenceddate': locdict.get('georeferenceddate'),
        'bels_georeferenceprotocol': locdict.get('georeferenceprotocol'),
        'bels_georeferencesources': locdict.get('georeferencesources'),
        'bels_georeferenceremarks': locdict.get('georeferenceremarks'),
        'bels_georeference_score': georeference_score(locdict),
        'bels_georeference_source': 'original data',
        'bels_best_of_n_georeferences': 1,
        'bels_match_type': 'original georeference'
    }


def country_fields(header):
    """
    Return in-order list of fields present among
    interpreted_countrycode, countrycode, v_countrycode, country.
    """
    candidates = ['interpreted_countrycode', 'countrycode', 'v_countrycode', 'country']
    return [f for f in candidates if f in header] or None


class BELS_Client:
    def __init__(self, pg_conn=None):
        self.conn = pg_conn or get_pg_connection()
        # For DDL and COPY operations:
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.countrycode_dict = {}

    def populate(self, table_name=None):
        """Load country code lookup into memory."""
        tn = table_name or f"{PG_SCHEMA_VOCABS}.countrycode_lookup"
        q = f"SELECT u_country, countrycode FROM {tn};"
        rows = run_pg_query(self.conn, q)
        for r in rows:
            self.countrycode_dict[r['u_country']] = r['countrycode']

    def country_report(self, count=0):
        if not self.countrycode_dict:
            print("Countrycode dictionary not populated.")
            return
        print(f"Loaded {len(self.countrycode_dict)} rows for countrycode lookup.")
        if count > 0:
            for i, (k, v) in enumerate(self.countrycode_dict.items()):
                if i >= count:
                    break
                print(f"{k}: {v}")
        else:
            print(self.countrycode_dict)

    def get_best_countrycode(self, locdict):
        if locdict is None:
            return None
        for key in ['interpreted_countrycode', 'countrycode', 'v_countrycode', 'country']:
            val = locdict.get(key)
            if val:
                return self.countrycode_dict.get(val.upper())
        return None


# --- Query builders for PostgreSQL ---

def query_location_by_id(loc_base64, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.locations_distinct_with_scores"
    return f"""
SELECT
    encode(dwc_location_hash, 'base64') AS locationid,
    *
FROM
    {tn}
WHERE
    encode(dwc_location_hash, 'base64') = '{loc_base64}'
"""


def query_location_by_hashid(loc_hash, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.locations_distinct_with_scores"
    return f"""
SELECT
    dwc_location_hash AS locationid,
    *
FROM
    {tn}
WHERE
    dwc_location_hash = '{loc_hash}'
"""


def query_best_sans_coords_georef(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_sans_coords_best_georef"
    return f"""
SELECT *
FROM {tn}
WHERE matchme_sans_coords = '{matchstr}'
"""


def query_best_sans_coords_georef_reduced(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_sans_coords_best_georef"
    return f"""
SELECT 
  interpreted_countrycode    AS bels_countrycode,
  matchme_sans_coords         AS bels_match_string,
  interpreted_decimallatitude  AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  'epsg:4326'                 AS bels_geodeticdatum,
  ROUND(unc_numeric)::INT      AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby            AS bels_georeferencedby,
  v_georeferenceddate          AS bels_georeferenceddate,
  v_georeferenceprotocol       AS bels_georeferenceprotocol,
  v_georeferencesources        AS bels_georeferencesources,
  v_georeferenceremarks        AS bels_georeferenceremarks,
  georef_score                 AS bels_georeference_score,
  source                       AS bels_georeference_source,
  georef_count                 AS bels_best_of_n_georeferences,
  'match sans coords'          AS bels_match_type
FROM {tn}
WHERE matchme_sans_coords = '{matchstr}'
"""


def query_best_with_verbatim_coords_georef(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_verbatimcoords_best_georef"
    return f"""
SELECT *
FROM {tn}
WHERE matchme = '{matchstr}'
"""


def query_best_with_verbatim_coords_georef_reduced(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_verbatimcoords_best_georef"
    return f"""
SELECT 
  interpreted_countrycode    AS bels_countrycode,
  matchme                    AS bels_match_string,
  interpreted_decimallatitude  AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  'epsg:4326'                 AS bels_geodeticdatum,
  ROUND(unc_numeric)::INT      AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby            AS bels_georeferencedby,
  v_georeferenceddate          AS bels_georeferenceddate,
  v_georeferenceprotocol       AS bels_georeferenceprotocol,
  v_georeferencesources        AS bels_georeferencesources,
  v_georeferenceremarks        AS bels_georeferenceremarks,
  georef_score                 AS bels_georeference_score,
  source                       AS bels_georeference_source,
  georef_count                 AS bels_best_of_n_georeferences,
  'match using verbatim coords' AS bels_match_type
FROM {tn}
WHERE matchme = '{matchstr}'
"""


def query_best_with_coords_georef(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_with_coords_best_georef"
    return f"""
SELECT *
FROM {tn}
WHERE matchme_with_coords = '{matchstr}'
"""


def query_best_with_coords_georef_reduced(matchstr, table_name=None):
    tn = table_name or f"{PG_SCHEMA_GAZ}.matchme_with_coords_best_georef"
    return f"""
SELECT 
  interpreted_countrycode     AS bels_countrycode,
  matchme_with_coords         AS bels_match_string,
  interpreted_decimallatitude  AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  'epsg:4326'                 AS bels_geodeticdatum,
  ROUND(unc_numeric)::INT      AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby            AS bels_georeferencedby,
  v_georeferenceddate          AS bels_georeferenceddate,
  v_georeferenceprotocol       AS bels_georeferenceprotocol,
  v_georeferencesources        AS bels_georeferencesources,
  v_georeferenceremarks        AS bels_georeferenceremarks,
  georef_score                 AS bels_georeference_score,
  source                       AS bels_georeference_source,
  georef_count                 AS bels_best_of_n_georeferences,
  'match with coords'          AS bels_match_type
FROM {tn}
WHERE matchme_with_coords = '{matchstr}'
"""


# --- High-level getters that execute queries and return dicts ---

def get_location_by_id(pg_conn, loc_base64):
    rows = run_pg_query(pg_conn, query_location_by_id(loc_base64), max_results=1)
    return dict(rows[0]) if rows else None


def get_location_by_hashid(pg_conn, loc_hash):
    rows = run_pg_query(pg_conn, query_location_by_hashid(loc_hash), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_sans_coords_georef(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_sans_coords_georef(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_sans_coords_georef_reduced(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_sans_coords_georef_reduced(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_with_verbatim_coords_georef(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_with_verbatim_coords_georef(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_with_verbatim_coords_georef_reduced(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_with_verbatim_coords_georef_reduced(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_with_coords_georef(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_with_coords_georef(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


def get_best_with_coords_georef_reduced(pg_conn, matchstr):
    rows = run_pg_query(pg_conn, query_best_with_coords_georef_reduced(matchstr), max_results=1)
    return dict(rows[0]) if rows else None


# --- Table operations adapted for PostgreSQL ---

def delete_table(pg_conn, table_name):
    """Drop a table if it exists."""
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    pg_conn.commit()


def import_table(pg_conn, csv_path, header, table_name=None):
    """
    Load a local CSV into Postgres using COPY.
    csv_path: filesystem path to CSV
    header: list of column names
    """
    if not csv_path or not header:
        logging.error("No CSV path or header provided.")
        return None
    tn = table_name or os.path.splitext(os.path.basename(csv_path))[0]
    cols = ', '.join(header)
    with pg_conn.cursor() as cur, open(csv_path, 'r') as f:
        cur.copy_expert(f"COPY {tn} ({cols}) FROM STDIN WITH CSV HEADER", f)
    pg_conn.commit()
    return tn


def export_table(pg_conn, table_name, file_path):
    """
    Export a table to a local CSV file using COPY.
    file_path: filesystem path for output CSV
    """
    with pg_conn.cursor() as cur, open(file_path, 'w') as f:
        cur.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
    return file_path


def process_import_table(pg_conn, input_table_id, countryfieldlist):
    """
    PostgreSQL implementation of the BigQuery georeference pipeline.
    - input_table_id: string like 'belsapi.my_table'
    - countryfieldlist: list of one or more of
        ['interpreted_countrycode','countrycode','v_countrycode','country']
    Returns the name of the output table in results schema.
    """
    if not input_table_id or not countryfieldlist:
        return None

    # extract just the table name (without schema)
    _, table_name = input_table_id.split('.', 1)
    output_table = f"{PG_SCHEMA_OUTPUT}.{table_name}"

    # build a COALESCE clause in priority order
    priority = ['interpreted_countrycode','countrycode','v_countrycode','country']
    present = [f for f in priority if f in countryfieldlist]
    if not present:
        return None
    matchcountry = f"COALESCE({', '.join(present)}) AS bels_match_country"

    # we wrap everything in a single transaction to avoid stray temp tables
    with pg_conn.cursor() as cur:
        # 1) Create a temp table 'countrify'
        cur.execute(f"""
            CREATE TEMPORARY TABLE countrify ON COMMIT DROP AS
            SELECT *, {matchcountry}
              FROM {input_table_id};
        """)

        # 2) Join to countrycode_lookup to get bels_interpreted_countrycode + a UUID
        cur.execute(f"""
            CREATE TEMPORARY TABLE interpreted ON COMMIT DROP AS
            SELECT
              c.*,
              v.countrycode  AS bels_interpreted_countrycode,
              gen_random_uuid() AS bels_id
            FROM countrify c
            LEFT JOIN {PG_SCHEMA_VOCABS}.countrycode_lookup v
              ON UPPER(c.bels_match_country) = v.u_country;
        """)

        # 3) Build your matcher table
        #    -- you must replace the normalize_* calls with your own SQL or Python logic
        cur.execute(f"""
            CREATE TEMPORARY TABLE matcher ON COMMIT DROP AS
            SELECT
              i.bels_id,
              -- placeholder: replace these with your chosen normalization functions
              regexp_replace(i.bels_match_country, '\\s+', '', 'g') AS bels_matchwithcoords,
              regexp_replace(i.bels_match_country, '\\s+', '', 'g') AS bels_matchverbatimcoords,
              regexp_replace(i.bels_match_country, '\\s+', '', 'g') AS bels_matchsanscoords
            FROM interpreted i;
        """)

        # 4) Seed georefs from the coords match table
        cur.execute(f"""
            CREATE TEMPORARY TABLE georefs ON COMMIT DROP AS
            SELECT
              m.bels_id,
              b.interpreted_decimallatitude   AS bels_decimallatitude,
              b.interpreted_decimallongitude  AS bels_decimallongitude,
              CASE WHEN b.interpreted_decimallatitude IS NULL
                   THEN NULL ELSE 'epsg:4326' END  AS bels_geodeticdatum,
              ROUND(b.unc_numeric)::INT        AS bels_coordinateuncertaintyinmeters,
              b.v_georeferencedby              AS bels_georeferencedby,
              b.v_georeferenceddate            AS bels_georeferenceddate,
              b.v_georeferenceprotocol         AS bels_georeferenceprotocol,
              b.v_georeferencesources          AS bels_georeferencesources,
              b.v_georeferenceremarks          AS bels_georeferenceremarks,
              b.georef_score                   AS bels_georeference_score,
              b.source                         AS bels_georeference_source,
              b.georef_count                   AS bels_best_of_n_georeferences,
              'match using coords'            AS bels_match_type
            FROM matcher m
            JOIN {PG_SCHEMA_GAZ}.matchme_with_coords_best_georef b
              ON m.bels_matchwithcoords = b.matchme_with_coords;
        """)

        # 5) Append verbatim-coords matches
        cur.execute(f"""
            INSERT INTO georefs
            SELECT
              m.bels_id,
              b.interpreted_decimallatitude   AS bels_decimallatitude,
              b.interpreted_decimallongitude  AS bels_decimallongitude,
              CASE WHEN b.interpreted_decimallatitude IS NULL
                   THEN NULL ELSE 'epsg:4326' END  AS bels_geodeticdatum,
              ROUND(b.unc_numeric)::INT        AS bels_coordinateuncertaintyinmeters,
              b.v_georeferencedby              AS bels_georeferencedby,
              b.v_georeferenceddate            AS bels_georeferenceddate,
              b.v_georeferenceprotocol         AS bels_georeferenceprotocol,
              b.v_georeferencesources          AS bels_georeferencesources,
              b.v_georeferenceremarks          AS bels_georeferenceremarks,
              b.georef_score                   AS bels_georeference_score,
              b.source                         AS bels_georeference_source,
              b.georef_count                   AS bels_best_of_n_georeferences,
              'match using verbatim coords'   AS bels_match_type
            FROM matcher m
            JOIN {PG_SCHEMA_GAZ}.matchme_verbatimcoords_best_georef b
              ON m.bels_matchverbatimcoords = b.matchme
            WHERE m.bels_id NOT IN (SELECT bels_id FROM georefs);
        """)

        # 6) Append sans-coords matches
        cur.execute(f"""
            INSERT INTO georefs
            SELECT
              m.bels_id,
              b.interpreted_decimallatitude   AS bels_decimallatitude,
              b.interpreted_decimallongitude  AS bels_decimallongitude,
              CASE WHEN b.interpreted_decimallatitude IS NULL
                   THEN NULL ELSE 'epsg:4326' END  AS bels_geodeticdatum,
              ROUND(b.unc_numeric)::INT        AS bels_coordinateuncertaintyinmeters,
              b.v_georeferencedby              AS bels_georeferencedby,
              b.v_georeferenceddate            AS bels_georeferenceddate,
              b.v_georeferenceprotocol         AS bels_georeferenceprotocol,
              b.v_georeferencesources          AS bels_georeferencesources,
              b.v_georeferenceremarks          AS bels_georeferenceremarks,
              b.georef_score                   AS bels_georeference_score,
              b.source                         AS bels_georeference_source,
              b.georef_count                   AS bels_best_of_n_georeferences,
              'match sans coords'             AS bels_match_type
            FROM matcher m
            JOIN {PG_SCHEMA_GAZ}.matchme_sans_coords_best_georef b
              ON m.bels_matchsanscoords = b.matchme_sans_coords
            WHERE m.bels_id NOT IN (SELECT bels_id FROM georefs);
        """)

        # 7) Finally, write out to your persistent results table
        cur.execute(f"""
            DROP TABLE IF EXISTS {output_table};
            CREATE TABLE {output_table} AS
            SELECT
              m.bels_matchwithcoords,
              m.bels_matchverbatimcoords,
              m.bels_matchsanscoords,
            FROM interpreted i
            JOIN matcher     m ON i.bels_id = m.bels_id
            LEFT JOIN georefs g ON m.bels_id = g.bels_id;
        """)

    # commit everything
    pg_conn.commit()
    return output_table



def row_as_list(row):
    ''' Get row data as a list.
    parameters:
        row - an instance of row data from a table.
    returns:
        row_list - the row as a list of (key, value) tuples.
    '''
    row_list = list(row.items())
    return row_list

def row_as_dict(row):
    ''' Get row data as a dict.
    parameters:
        row - an instance of row data from a table.
    returns:
        row_dict - the row as a dict.
    '''

    row_dict = {}
    for item in row_as_list(row):
        row_dict[item[0]]=item[1]
    return row_dict


def row_as_json(row):
    """Serialize a row (dict-like) to JSON using CustomJsonEncoder."""
    return json.dumps(row, cls=CustomJsonEncoder)


