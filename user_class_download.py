from google.cloud import bigquery
import os
from datetime import datetime, timedelta
from multiprocessing import Pool
from functools import partial

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    os.getenv("HOME"), "work/behavior-catcher-9adb879ec19d.json"
)

client = bigquery.Client()


def download_data(base_date, day_offset):
    current_date = base_date + timedelta(days=day_offset)
    next_date = current_date + timedelta(days=1)

    QUERY = f"""
        SELECT date, cust_id, site_id, currency_id, latest_cc, tag_list
        FROM `behavior-catcher.behavior_catcher_dm.custlist_daily` 
        WHERE date = DATETIME("{current_date.strftime('%Y-%m-%d')}") 

    """
    print(f"start to download data for {current_date.strftime('%Y-%m-%d')}")
    query_job = client.query(QUERY)  # API request
    df = query_job.to_dataframe()
    print(f"len(df) for {current_date.strftime('%Y-%m-%d')}: {len(df)}")
    filename = os.path.join(
        os.getenv("HOME"), f'work/behavior/class_{current_date.strftime("%Y%m%d")}.csv'
    )
    df.to_csv(filename, index=False)

    print(f'Data for {current_date.strftime("%Y-%m-%d")} saved to {filename}')


if __name__ == "__main__":
    print("start")
    base_date = datetime(2024, 6, 1)
    download_data_with_base = partial(download_data, base_date)
    with Pool(processes=30) as pool:
        pool.map(download_data_with_base, range(30))
