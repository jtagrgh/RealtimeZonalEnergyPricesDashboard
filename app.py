import streamlit as st
import pandas as pd
from common import get_csv_from_gcp


@st.cache_data
def get_all_data():
    # df = pd.read_csv(data.save_data_fname, parse_dates=True, index_col=0)
    df = get_csv_from_gcp()
    df['poll_time_utc'] = pd.to_datetime(df['poll_time_utc'])
    df.sort_index(inplace=True)
    return df


def get_filtered_data(data_df, start, end, columns, sample_period):
    start = pd.Timestamp(start)
    end = pd.Timestamp(end) + pd.Timedelta(days=1)
    filtered_df = data_df.loc[(data_df.index >= start) & (data_df.index < end)]
    filtered_df = filtered_df[columns]
    filtered_df = filtered_df.resample(sample_period).mean()
    return filtered_df


all_data = get_all_data().sort_index()

st.title('IESO Realtime Zonal Energy Prices')

last_poll_utc = all_data.loc[all_data.index.max()]['poll_time_utc'] if not all_data.empty and 'poll_time_utc' in all_data.columns else None
last_poll_str = last_poll_utc.strftime('%Y/%m/%d %H:%M:%S') if last_poll_utc is not None else ''

last_poll_link = all_data.loc[all_data.index.max()]['link'] if not all_data.empty and 'link' in all_data.columns else ''

st.write(f'Last updated: {last_poll_str}')
st.write(f'Polled from: {last_poll_link}')

st.divider()

all_data.columns = [col.replace(':', '_') for col in all_data.columns]

min_date = all_data.index.min().to_pydatetime()
max_date = all_data.index.max().to_pydatetime()

sel_dates = st.date_input('Date Range', (max_date, max_date), min_date, max_date)
sel_dates = sel_dates if len(sel_dates) == 2 else 2*sel_dates if len(sel_dates) == 1 else (max_date, max_date)

data_columns = all_data.columns.drop('link')
first_col = data_columns[0] if len(data_columns) >= 1 else None

sel_columns = st.pills('Regions', data_columns, selection_mode='multi', default=first_col)
sel_columns = sel_columns if sel_columns is not None else []

sel_sample = st.segmented_control('Sample Period', ['5min', '12h', '1h', '1d'], default='5min')
sel_sample = sel_sample if sel_sample is not None else '1h' 

st.divider()

sel_show_data = st.toggle('Show Data', value=True)

if len(sel_columns) == 0:
    st.write('No Regions selected.')

filtered_data = get_filtered_data(all_data, sel_dates[0], sel_dates[1], sel_columns, sel_sample)\

if sel_show_data:
    st.dataframe(filtered_data)

sel_show_chart = st.toggle('Show Chart')

if sel_show_chart:
    st.line_chart(filtered_data)