import streamlit as st
import pandas as pd
import json
from data import save_missing_data, get_date
import threading
import datetime


saved_data_name = 'saved_data'


if saved_data_name not in st.session_state:
    st.session_state[saved_data_name] = pd.DataFrame()


def save_df_session(df, link):
    st.session_state[saved_data_name] = df
    status = st.session_state['status']
    date = get_date(link)
    print(date)
    status.write(f'Downloaded: {date}')


def get_df_session() -> pd.DataFrame:
    return st.session_state[saved_data_name]


def process_data(df: pd.DataFrame, sample_period, columns, reverse):
    processed_df = df[columns].resample(sample_period).mean()
    processed_df.sort_index(ascending=not reverse, inplace=True)
    return processed_df


st.title('IESO Realtime Zonal Energy Prices')

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

with st.form('Date Filter Form'):
    sel_dates = st.date_input('Date Range', (yesterday, today), max_value=today)
    submitted = st.form_submit_button('Get Data')
    if submitted and len(sel_dates) >= 2:
        min_date = pd.Timestamp(sel_dates[0])
        max_date = pd.Timestamp(sel_dates[1])
        st.session_state['status'] = st.status('Download Status')
        save_missing_data(get_df_session, save_df_session, min_date, max_date)
        st.session_state['status'].update(state='complete')

st.divider()

if get_df_session().empty:
    st.stop()

all_data = get_df_session().copy()
all_data.index.name = 'timestamp'

if 'link' not in all_data.columns:
    st.error('Missing <link> column')
    st.stop()

if 'poll_time_utc' not in all_data.columns:
    st.error('Missing <poll_time_utc> column')
    st.stop()

all_data.columns = [col.replace(':', '_') for col in all_data.columns]
data_columns = all_data.columns.drop(['link', 'poll_time_utc'])
first_col = data_columns[0] if len(data_columns) >= 1 else None

sel_columns = st.pills('Regions', data_columns, selection_mode='multi', default=first_col)
sel_columns = sel_columns if sel_columns is not None else []

sel_sample = st.segmented_control('Sample Period', ['5min', '1h', '12h', '1d'], default='5min')
sel_sample = sel_sample if sel_sample is not None else '5min'

sel_reverse = st.checkbox('Reverse order')

processed_data = process_data(all_data, sel_sample, sel_columns, sel_reverse)

st.dataframe(processed_data)

sel_show_chart = st.toggle('Show Chart')

if sel_show_chart:
    st.line_chart(processed_data)
