import argparse
import pandas as pd
from sqlalchemy import create_engine

# Menampilkan semua kolom dalam DataFrame saat dicetak
pd.options.display.max_columns = None


# Function untuk mendapatkan ID terakhir dari tabel SQLite.
def get_last_id(table_name, db_path="sqlite:///imdb.db"):
    engine = create_engine(db_path)

    with engine.connect() as conn:
        # Memeriksa apakah tabel ada di database
        table = conn.execute(
            f"""
            SELECT
                name
            FROM sqlite_master
            WHERE type = 'table' AND name = '{table_name}'
        """
        )

        if table.first() is not None:
            # Mendapatkan ID maksimum dari tabel jika tabel ada
            id = conn.execute(f"SELECT MAX(id) FROM {table_name}")
            return id.first()[0]

        # Mengembalikan None jika tabel tidak ada
        return None


# Function untuk mengekstrak data dari file JSON.
def extract_to_staging(filename, staging_filename, mode, last_id=None):
    df = pd.read_json(filename)

    if mode == "incremental" and last_id is not None:
        # Memfilter data untuk hanya mengambil data yang ID-nya lebih besar dari last_id
        df = df[df.id > last_id]

    # Menyimpan DataFrame ke file staging CSV
    df.to_csv(staging_filename, index=False)


# Function untuk me-load DataFrame ke dalam tabel SQLite.
def load_from_staging(staging_filename, table_name, mode, db_path="sqlite:///imdb.db"):
    engine = create_engine(db_path)

    # Mengganti tabel jika mode full atau menambahkan data ke tabel jika mode incremental
    if mode == "incremental" and last_id is not None:
        write_disposition = "append"
    else:
        write_disposition = "replace"

    with engine.connect() as conn:
        # Membaca data dari file staging CSV ke DataFrame
        df = pd.read_csv(staging_filename)

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

    # Nama file staging
    staging_filename = f"staging_{args.dest_table_name}_{args.mode}_{last_id}.csv"

    # Extract data dari file JSON
    extract_to_staging(args.source_filename, staging_filename, args.mode, last_id)
    print("Extract berhasil")

    # Load data ke SQLite
    load_from_staging(staging_filename, args.dest_table_name, args.mode)
    print("Load berhasil")

# contoh:
# python staging_area_file.py --source_filename posts.json --dest_table_name posts --mode full
