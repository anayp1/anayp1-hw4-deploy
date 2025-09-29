# Created by <Your Name> for CS1060 HW4.
# Portions of this file were generated with the help of ChatGPT (OpenAI) and adapted by me.

import os
import re
import sqlite3
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# ----- Database path (explicit, absolute, read-only) -----
APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "data.db")          # data.db sits next to this file
DB_URI = f"file:{DB_PATH}?mode=ro"                  # open read-only for safety

# Allowed measure names (exact strings from the assignment)
ALLOWED_MEASURES = {
    "Violent crime rate",
    "Unemployment",
    "Children in poverty",
    "Diabetic screening",
    "Mammography screening",
    "Preventable hospital stays",
    "Uninsured",
    "Sexually transmitted infections",
    "Physical inactivity",
    "Adult obesity",
    "Premature Death",
    "Daily fine particulate matter",
}

ZIP_RE = re.compile(r"^\d{5}$")


def get_db():
    """
    Open the SQLite DB in read-only mode, fail fast if the file is missing.
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB missing at {DB_PATH}")
    conn = sqlite3.connect(DB_URI, uri=True, timeout=2.0)  # short lock wait
    conn.row_factory = sqlite3.Row
    return conn


def query_rows(zip_code: str, measure_name: str):
    """
    Faster two-step query:
      1) Get (county, state_abbreviation) for the ZIP from zip_county.
      2) For each state, fetch CHR rows for that measure and the matching county names.
    This avoids scanning CHR with an EXISTS subquery.
    """
    with get_db() as conn:
        # Step 1: which counties map to this ZIP?
        zrows = conn.execute(
            """
            SELECT DISTINCT county, state_abbreviation AS state
            FROM zip_county
            WHERE zip = ?
            """,
            (zip_code,),
        ).fetchall()

        if not zrows:
            return []

        # Group counties by state to keep parameters small
        by_state = {}
        for r in zrows:
            by_state.setdefault(r["state"], set()).add(r["county"])

        results = []
        for state, counties in by_state.items():
            county_list = sorted(counties)
            placeholders = ",".join(["?"] * len(county_list))
            params = [measure_name, state] + county_list
            sql = f"""
                SELECT
                  c.state  AS state,
                  c.county AS county,
                  c.state_code AS state_code,
                  c.county_code AS county_code,
                  c.year_span AS year_span,
                  c.measure_name AS measure_name,
                  c.measure_id AS measure_id,
                  c.numerator AS numerator,
                  c.denominator AS denominator,
                  c.raw_value AS raw_value,
                  c.confidence_interval_lower_bound AS confidence_interval_lower_bound,
                  c.confidence_interval_upper_bound AS confidence_interval_upper_bound,
                  c.data_release_year AS data_release_year,
                  c.fipscode AS fipscode
                FROM county_health_rankings AS c
                WHERE c.measure_name = ?
                  AND c.state = ?
                  AND c.county IN ({placeholders})
                ORDER BY CAST(c.data_release_year AS INTEGER), CAST(c.year_span AS INTEGER)
            """
            rows = conn.execute(sql, params).fetchall()
            results.extend(dict(r) for r in rows)

        return results


# ----- Error handlers -> always JSON -----
@app.errorhandler(400)
def bad_request(e):
    return jsonify(error="bad request"), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify(error="not found"), 404


@app.errorhandler(418)
def teapot(e):
    return jsonify(error="teapot"), 418


# ----- API endpoint -----
@app.post("/county_data")
def county_data():
    # Must be JSON
    if not request.is_json:
        abort(400)

    data = request.get_json(silent=True) or {}

    # Special case: coffee=teapot -> 418 (overrides everything)
    if data.get("coffee") == "teapot":
        abort(418)

    # Required fields
    zip_code = data.get("zip")
    measure_name = data.get("measure_name")

    if not zip_code or not measure_name:
        abort(400)

    # Basic validation
    if not isinstance(zip_code, str) or not ZIP_RE.match(zip_code):
        abort(400)
    if not isinstance(measure_name, str) or measure_name not in ALLOWED_MEASURES:
        abort(400)

    # Query
    results = query_rows(zip_code, measure_name)
    if not results:
        abort(404)

    return jsonify(results), 200


# Local dev only (Render uses gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))