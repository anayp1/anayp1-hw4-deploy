import csv
import sqlite3
import sys
import os

def csv_to_sqlite(db_name, csv_file):
    # Table name comes from the CSV file name (e.g., "zip_county.csv" -> "zip_county")
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Read CSV (assumes headers are valid SQL identifiers: no spaces/quotes)
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Recreate the table each run so it's deterministic
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        # All columns as TEXT (simple, robust)
        columns = ", ".join([f"{col} TEXT" for col in headers])
        create_table_sql = f"CREATE TABLE {table_name} ({columns});"
        cur.execute(create_table_sql)

        # Prepare insert
        placeholders = ", ".join(["?"] * len(headers))
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        # Insert rows
        for row in reader:
            # Pad/trim just in case a row has fewer/more columns
            if len(row) != len(headers):
                row = (row + [""] * len(headers))[:len(headers)]
            cur.execute(insert_sql, row)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 csv_to_sqlite.py <database_name> <csv_file>")
        sys.exit(1)

    db_name = sys.argv[1]
    csv_file = sys.argv[2]
    csv_to_sqlite(db_name, csv_file)