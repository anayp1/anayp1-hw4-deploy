# Created by <Your Name> for CS1060 HW4.
# Portions of this file were generated with the help of ChatGPT (OpenAI) and adapted by me.

import os
import re
import sqlite3
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# ----- Database path (explicit, absolute) -----
APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "data.db")                 # data.db sits next to this file
DB_URI = f"file:{DB_PATH}?mode=ro"                         # open read-only for safety

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
    Using sqlite URI ensures no accidental writes and guards against destructive SQL.
    """
    if not os.path.exists(DB_PATH):
        # This makes any path issues obvious in Render logs
        raise FileNotFoundError(f"DB missing at {DB_PATH}")
    conn = sqlite3.connect(DB_URI, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


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

    # Basic input validation/sanitization
    if not isinstance(zip_code, str) or not ZIP_RE.match(zip_code):
        # 5-digit ZIP only
        abort(400)

    if not isinstance(measure_name, str):
        abort(400)

    # Enforce the whitelist exactly (per spec wording "should be one of")
    if measure_name not in ALLOWED_MEASURES:
        abort(400)

    # Parameterized, injection-safe query
    # We return columns with lower_snake_case names to match the example JSON.
    sql = """
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
      AND EXISTS (
        SELECT 1
        FROM zip_county AS z
        WHERE z.zip = ?
          AND z.county = c.county
          AND z.state_abbreviation = c.state
      )
    ORDER BY CAST(c.data_release_year AS INTEGER), CAST(c.year_span AS INTEGER)
    """

    with get_db() as conn:
        cur = conn.execute(sql, (measure_name, zip_code))
        rows = cur.fetchall()

    if not rows:
        # pair exists in neither table / no join match
        abort(404)

    # Convert sqlite3.Row -> dict
    result = [dict(r) for r in rows]
    return jsonify(result), 200


# For local dev only (Render will use gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))