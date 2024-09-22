import argparse
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Menampilkan semua kolom dalam DataFrame saat dicetak
pd.options.display.max_columns = None


# Function untuk mendapatkan ID terakhir dari tabel SQLite.
def get_last_id(table_name, db_path="sqlite:///imdb.db"):
    engine = create_engine(db_path)

    with engine.connect() as conn:
        # Memeriksa apakah tabel ada di database
        table = conn.execute(
            text(
                f"""
            SELECT
                name
            FROM sqlite_master
            WHERE type = 'table' AND name = :table_name
            """
            ),
            {"table_name": table_name},
        )

        if table.first() is not None:
            # Mendapatkan ID maksimum dari tabel jika tabel ada
            id = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
            return id.first()[0]

        # Mengembalikan None jika tabel tidak ada
        return None


# Function untuk mengekstrak data dari file JSON.
def extract(filename, mode, last_id=None):
    df = pd.read_json(filename)

    if mode == "incremental" and last_id is not None:
        # Memfilter data untuk hanya mengambil data yang ID-nya lebih besar dari last_id
        df = df[df.id > last_id]

    return df


# Function untuk me-load DataFrame ke dalam tabel SQLite.
def load(df, table_name, mode, db_path="sqlite:///imdb.db"):
    engine = create_engine(db_path)

    # Mengganti tabel jika mode full atau menambahkan data ke tabel jika mode incremental
    if mode == "incremental" and last_id is not None:
        write_disposition = "append"
    else:
        write_disposition = "replace"

    with engine.connect() as conn:
        # Memuat DataFrame ke dalam tabel SQLite
        df.to_sql(table_name, conn, index=False, if_exists=write_disposition)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_filename")
    parser.add_argument("--dest_table_name")
    parser.add_argument("--mode", choices=["full", "incremental"])
    args = parser.parse_args()

    # Mendapatkan ID terakhir jika mode incremental
    if args.mode == "incremental":
        last_id = get_last_id(args.dest_table_name)
        print(f"last id: {last_id}")
    else:
        last_id = None

    # Extract data dari file JSON
    df = extract(args.source_filename, args.mode, last_id)
    print(df)
    print("Extract berhasil")

    # Load data ke SQLite
    load(df, args.dest_table_name, args.mode)
    print("Load berhasil")

# contoh:
# python ingestion_mode.py --source_filename posts.json --dest_table_name posts --mode full
