import zipfile

import requests
import pandas as pd
import psycopg2
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values

SHIP_NAMES = [
    "CSL MANHATTAN",
    "CSL NIAGARA",
    "CSL TADOUSSAC",
    "CSL SANTA MARIA",
    "CSL ASSINIBOINE",
    "MSC ALTAIR",
    "MSC AURORA",
    "MSC FRANCESCA",
    "MSC DANIELA",
    "MSC JEANNE",
    "MSC CAMILLE",
    "MSC ARIES",
    "MSC CELINE",
    "MSC EVA",
    "MSC KATIE",
    "GOLIATH",
    "MSC AQUARIUS"
]

INDEX_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2022/index.html"
BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2022/"

DB_NAME = "ship_data"
DB_USER = "postgres"
DB_PASSWORD = "Da$54338012345"
DB_HOST = "localhost"
DB_PORT = "5432"


def get_available_files():
    response = requests.get(INDEX_URL)

    if response.status_code != 200:
        print("Failed to fetch index page")
        return []
    else:
        print("Index page loaded")

    soup = BeautifulSoup(response.text, "html.parser")
    links = [a['href']for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
    return links

def fetch_and_extract_csv(file_name):
    url = BASE_URL + file_name
    response = requests.get(url)

    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"No csv found in {file_name}")
                return None
            with zip_ref.open(csv_files[0]) as csv_file:
                df = pd.read_csv(csv_file)
                df = df[df['VesselName'].isin(SHIP_NAMES)]
                integer_columns = ['Length', 'Width', 'Heading', 'VesselType', 'Status']
                for col in integer_columns:
                    if col in df.columns:
                        # First convert to numeric, handling any non-numeric values
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        # Replace NaN with 0
                        df[col] = df[col].fillna(0)
                        # Ensure values are within INTEGER range
                        df[col] = df[col].clip(-2147483648, 2147483647)
                        # Convert to integer
                        df[col] = df[col].astype('int32')
                df['Width'] = df['Width'].fillna(0)
                df['Length'] = df['Length'].fillna(0)
                df['Draft'] = df['Draft'].fillna(0)

                return df
    else:
        print(f"Failed to download {file_name}")
        return None



def upload_to_postgre(df, table_name="ships"):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )

        cur = conn.cursor()

        columns = list(df.columns)
        print("in postgre function")
        print(columns)

        col_str = ", ".join(columns)
        values_placeholder =", ".join(["%s"] * len(columns))

        values = [tuple(row) for row in df.to_numpy()]
        insert_query = f"INSERT INTO {table_name} ({col_str}) VALUES %s"
        execute_values(cur, insert_query, values)

        conn.commit()
        cur.close()
        print(f"Data uploaded to {table_name} successfully")
    except Exception as e:
        print(f"Database error: {e}")

    finally:
        if conn:
            conn.close()


def main():

    files_to_process = get_available_files()

    start_date = datetime(2022,10,6)
    end_date = datetime(2023,12,31)

    while start_date <= end_date:
        string_date = f'AIS_{start_date.year}_{start_date.month:02d}_{start_date.day:02d}.zip'

        if string_date in files_to_process:
            print(f"processing file for {string_date}")
            df = fetch_and_extract_csv(string_date)
            if df is not None and not df.empty:
                upload_to_postgre(df)

        else:
            print(f"skipping {string_date}, not found")

        start_date += timedelta(days=1)



if __name__ == "__main__":
    main()