import pandas as pd
import re
from time import sleep
from bs4 import BeautifulSoup
from urllib.request import urlopen
from common import *


def getdata():
    sleep(10)
    return pd.DataFrame([[1,2,3],[4,5,6]])


def floatna(str):
    return float(str) if str else None


def get_date(link):
    date_str = re.search(r'PUB_RealtimeZonalEnergyPrices_([0-9]+).xml', link).group(1)
    date_parts = [int(x) for x in re.match(r'(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2}).*', date_str).groups()]
    date_start = pd.Timestamp(year=date_parts[0], month=date_parts[1], day=date_parts[2], hour=date_parts[3]-1)
    return date_start


def get_hour_index(date_start):
    date_index = pd.date_range(start=date_start, end=date_start+pd.Timedelta(hours=1), freq='5min', inclusive='left')
    return date_index


def hour_df(link):
    page_xml = BeautifulSoup(urlopen(link), 'xml')
    zone_names = [tag.text for tag in page_xml.find_all('ZoneName')]
    interval_prices = [(int(tag.Interval.text), floatna(tag.ZonalPrice.text)) for tag in page_xml.find_all('IntervalPrice')]

    date_start = get_date(link)
    hour_index = pd.date_range(start=date_start, end=date_start+pd.Timedelta(hours=1), freq='5min', inclusive='left')

    hour_df = pd.DataFrame(interval_prices, columns=['interval', 'price'])\
        .groupby('interval').agg(list) \
        .pipe(lambda df: pd.DataFrame(df['price'].to_list())) \
        .set_axis(zone_names, axis=1) \
        .set_index(hour_index) \
        .rename_axis('interval')
  
    return hour_df


def price_links():
    root_link = 'https://reports-public.ieso.ca/public/RealtimeZonalEnergyPrices/'
    main_page = urlopen(root_link)
    bs_main = BeautifulSoup(main_page, 'html.parser')
    price_tags = bs_main.find_all('a', attrs={'href': re.compile('PUB_RealtimeZonalEnergyPrices_[0-9]+.xml')})
    price_links = [root_link + tag['href'] for tag in price_tags]
    return price_links


def save_missing_data(get_data_fn, save_data_fn, chunk_size, min_date, max_date):
    saved_df = get_data_fn()
    
    saved_links = saved_df['link'].to_list() if 'link' in saved_df.columns else []
    available_links = price_links()
    missing_links = [link for link in available_links if link not in set(saved_links)] + [available_links[-1]]
    filtered_links = [link for link in missing_links if (min_date <= get_date(link) < max_date + pd.Timedelta(days=1))]

    chunk_count = 0

    for link in filtered_links:
        # print(f'Getting data from {link} ...')
        df = hour_df(link)
        # print('Done.')
        df['link'] = [link]*df.shape[0]
        df['poll_time_utc'] = [pd.Timestamp.utcnow()]*df.shape[0]
        saved_df = saved_df.combine_first(df)
        full_index = pd.date_range(saved_df.index.min(), saved_df.index.max(), freq='5min')
        saved_df = saved_df.reindex(full_index)
        if chunk_count % chunk_size == 0:
            save_data_fn(saved_df)
        chunk_count += 1

    save_data_fn(saved_df)


if __name__ == '__main__':
    save_missing_data(get_csv_from_local, save_df_local, 1, pd.Timestamp.now() - pd.Timedelta(days=1), pd.Timestamp.now())
