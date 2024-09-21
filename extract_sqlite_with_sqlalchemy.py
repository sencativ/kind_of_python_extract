import pandas as pd
import urllib.request
from sqlalchemy import create_engine

pd.options.display.max_columns = None


def extract_sqlite_with_sqlalchemy(url, filename, query):
    # Download database dari URL
    urllib.request.urlretrieve(url, filename)

    # Membuat koneksi ke database SQLite
    engine = create_engine(f"sqlite:///{filename}")
    connection = engine.connect().execution_options(stream_results=True)

    # Mengambil data dengan query SQL
    df = pd.read_sql(query, connection)

    # Menutup koneksi
    connection.close()
    engine.dispose()

    return df


# Panggil fungsi
url = "https://github.com/djv007/Project-IMDB-database/raw/master/IMDB.sqlite"
filename = "imdb.db"
query = "SELECT * FROM IMDB LIMIT 10"
df_sqlite = extract_sqlite_with_sqlalchemy(url, filename, query)

print(df_sqlite)
