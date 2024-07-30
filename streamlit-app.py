import streamlit as st
import time
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh


@st.cache_data
def fetch_voting_states():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("""
                SELECT COUNT(*) AS voters_count FROM voters;
                """)
    voters_count = cur.fetchone()[0]
    
    # Fetch total number of candidates
    cur.execute("""
                SELECT COUNT(*) AS candidates_count FROM candidates;
                """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers = 'localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer = lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages:
        for sum_message in message:
            data.append(sum_message.values)
    return data

def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    
    colors = plt.cm.viridis(np.linspace(1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel("Candidate")
    plt.ylabel("Total Votes")
    plt.title("Vote Counts per Candidate")
    plt.xticks(rotation=90)
    return plt

def plot_donut_chart(data):
    labels =list(data['candidate_name'])
    sizes = list(data['total_votes'])

    fig, ax = plt.subplot()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%'  , starttangle=140)
    ax.axis('equal')

    plt.title("Candidates Votes")
    return fig

@st.cache_data(show_spinner=False)
def spit_frame(input_df, rows):
    df = [input_df.loc[i: i + rows -1, :] for i in range(0, len(input_df), rows)]
    return df

def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort by", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆", "⬇"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆", ignore_index = True
        )
    pagination = st.container()

    bottom_menu = st.columns((4,1,1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) /batch_size) if int(len(table_data) / batch_size)>0 else 1
        )
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)    
            
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}**")
    
    pages = spit_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

    st.session_state['last_update'] = time.time()


def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 100, key='auto')


    if st.sidebar.button('Refresh Data'):
        update_data()

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at : {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch voting statistics from postgres
    voters_count, candidates_count = fetch_voting_states()

    # Display the statistics
    st.markdown("""____""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # Display the leading candidate information
    st.markdown("""___""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader('Total Vote: {}'.format(leading_candidate['total_votes']))

    # Display the statistics and visualizations
    st.markdown("""___""")
    st.header("Voting Statistics")
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)

    # Display the barchart and donut chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fit = plot_donut_chart(results)
        st.pyplot(donut_fit)
    
    st.table(results)


    # Fetch data from kafka on aggregated turnout by location 
    location_consumer = create_kafka_consumer('aggregated_turnout_by_location')
    location_data = fetch_data_from_kafka(location_consumer)

    location_result = pd.DataFrame(location_data)

    # max locaiton identificatio
    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    # Display the location base votes of voters
    st.header("Location of Voters")
    paginate_table(location_result)


st.title("Realtime Election Voting Dashboard")
topic_name = "aggregated_votes_per_candidate"

sidebar()   
update_data()