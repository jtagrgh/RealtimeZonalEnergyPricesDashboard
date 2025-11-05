import pandas as pd
import gcsfs
import os


save_data_fname = 'price_data'
save_data_with_ext = save_data_fname + '.csv'
blob_url = f'gs://ieso-zonal-data/{save_data_fname}.csv'


def get_csv_from_gcp(token):
    fs = gcsfs.GCSFileSystem(token=token)
    df = pd.read_csv(blob_url, storage_options={'token': token}, parse_dates=True, index_col=0) if fs.exists(blob_url) else pd.DataFrame()
    return df


def save_df_to_gcp(df: pd.DataFrame):
    df.to_csv(blob_url)


def get_csv_from_local():
    df = pd.read_csv(save_data_with_ext, parse_dates=True, index_col=0) if os.path.exists(save_data_with_ext) \
        else pd.DataFrame()
    return df


def save_df_local(df):
    df.to_csv(save_data_with_ext)
