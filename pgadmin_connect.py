import streamlit as st
import psycopg2
import pandas as pd
from sshtunnel import SSHTunnelForwarder

# Initialize connection


@st.cache_resource
def init_connection():
    # SSH server settings
    ssh_host = st.secrets["ssh"]["host"]
    ssh_port = st.secrets["ssh"]["port"]
    ssh_user = st.secrets["ssh"]["user"]
    ssh_password = st.secrets["ssh"]["password"]

    # Database settings
    db_host = st.secrets["postgres"]["host"]
    db_port = st.secrets["postgres"]["port"]
    db_name = st.secrets["postgres"]["dbname"]
    db_user = st.secrets["postgres"]["user"]
    db_password = st.secrets["postgres"]["password"]

    # Setup SSH tunnel
    server = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=(db_host, db_port)
    )
    server.start()

    # Connect to the database through the SSH tunnel
    conn = psycopg2.connect(
        host="localhost",
        port=server.local_bind_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    return conn, server


def close_connection(conn, server):
    conn.close()
    server.stop()
