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


def transform_uppercase(df):
    # Membuat judul berita menjadi huruf besar
    df.title = df.title.str.upper()

    return df


# Konfigurasi
url = "https://www.bbc.com/news"
table_name = "news"

# Extract data berita
df_news = extract_news(url)
print("Extract berhasil")

# Transformasi data berita menjadi huruf besar
df_news_transformed = transform_uppercase(df_news)
print("Transform berhasil")

# Load data berita ke SQLite
load_sqlite(df_news_transformed, table_name)
print("Load berhasil")
