import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def extract_news(url):
    # Mengambil konten halaman web
    response = requests.get(url)
    response.raise_for_status()  # Memeriksa jika permintaan berhasil

    # Menginisialisasi BeautifulSoup untuk parsing HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # Mengambil semua teks dari tag <h2> yang biasanya berisi judul berita
    data = [h2.get_text() for h2 in soup.find_all("h2")]

    # Membuat DataFrame dari data yang diekstraksi
    df = pd.DataFrame(data, columns=["title"])

    return df


def load_sqlite(df, table_name, db_path="sqlite:///imdb.db"):
    # Membuat engine SQLAlchemy untuk koneksi ke database SQLite
    engine = create_engine(db_path)

    # Memuat DataFrame ke tabel SQLite, menggantikan tabel jika sudah ada
    with engine.connect() as conn:
        df.to_sql(table_name, conn, index=False, if_exists="replace")


def transform_uppercase(raw_table_name, table_name, db_path="sqlite:///imdb.db"):
    # Membuat engine SQLAlchemy untuk koneksi ke database SQLite
    engine = create_engine(db_path)

    # Membuat tabel baru dengan judul berita dalam huruf besar
    with engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT UPPER(title) as title
            FROM {raw_table_name}
        """
        )


# Konfigurasi
url = "https://www.bbc.com/news"
raw_table_name = "raw_news"
table_name = "news"

# Extract data berita
df_news = extract_news(url)
print("Extract berhasil")

# Load data berita ke SQLite
load_sqlite(df_news, raw_table_name)
print("Load berhasil")

# Transformasi data berita menjadi huruf besar dan simpan ke tabel baru
transform_uppercase(raw_table_name, table_name)
print("Transform berhasil")
