import calendar
import streamlit as st
from pgadmin_connect import init_connection
import pandas as pd
import altair as alt
import base64  # Import base64 for encoding CSV

# Page configuration
st.set_page_config(
    page_title="CICS Dashboard",
    page_icon="🥑",
    layout="wide",
    initial_sidebar_state="expanded"
)

alt.themes.enable("dark")

# Initialize connection
try:
    conn, server = init_connection()
except Exception as e:
    st.error(f"Error initializing connection: {e}")
    conn, server = None, None

# Default empty dataframes
data1 = pd.DataFrame(columns=['status', 'product_weight', 'qcp5_timestamp'])
data2 = pd.DataFrame(
    columns=['clientcompany', 'country', 'product_weight', 'order_id', 'created_at'])
data3 = pd.DataFrame(columns=['reception_id', 'reception_qtyremoved', 'qcp1_qtyremoved', 'qcp2_qtyremoved', 'qcp3_qtyremoved',
                     'qcp4_weight_rejected', 'qcp5_weight_rejected', 'exporter_name', 'cropname', 'reception_qtyrejected', 'created_at'])

# Check if the connection was successful
if conn:
    st.write("# CICS Dashboard")

    # Perform query using st.experimental_memo
    @st.experimental_memo(ttl=600)
    def run_query(query):
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return []

    rows1 = run_query("""
        SELECT 
            qcp5_products.status AS qcp5status,
            qcp5_products.totalproductnetweight AS product_weight,
            qcp5_products.created_at AS qcp5_timestamp 
        FROM 
            qcp5_products
    """)

    rows2 = run_query("""
        SELECT 
            countries.countryname AS country,          
            clients.companyname AS clientcompany,
            packinglist_products.totalproductnetweight AS product_weight,
            packinglist_products.order_id AS order_id,
            packinglist_products.created_at AS created_at
        FROM 
            packinglist_products
        LEFT JOIN 
            orders ON packinglist_products.order_id = orders.id
        LEFT JOIN 
            clients ON orders.client_id = clients.id
        LEFT JOIN 
            countries ON clients.country_id = countries.id;
    """)

    rows3 = run_query("""
        SELECT 
            receptionform.id AS reception_id,
            reception_attributes.qtyremoved AS reception_qtyremoved,
            qcp1_attributes.qtyremoved AS qcp1_qtyremoved,
            qcp2_attributes.qtyremoved AS qcp2_qtyremoved,
            qcp3_attributes.qtyremoved AS qcp3_qtyremoved,
            qcp4_recommendations.weight_rejected AS qcp4_weight_rejected,
            qcp5_recommendations.weight_rejected AS qcp5_weight_rejected,
            exportcompanies.expname AS exporter_name,
            crops.cropname AS cropname,
            receptionform.quantityrejected AS reception_qtyrejected,
            receptionform.created_at AS created_at
        FROM 
            receptionform
        LEFT JOIN 
            reception_attributes ON reception_attributes.id = receptionform.id
        LEFT JOIN 
            qcp1_attributes ON qcp1_attributes.id = receptionform.id
        LEFT JOIN 
            qcp2_attributes ON qcp2_attributes.id = receptionform.id
        LEFT JOIN 
            qcp3_attributes ON qcp3_attributes.id = receptionform.id
        LEFT JOIN 
            qcp4_recommendations ON qcp4_recommendations.id = receptionform.id
        LEFT JOIN 
            qcp5_recommendations ON qcp5_recommendations.id = receptionform.id
        LEFT JOIN 
            exportcompanies ON exportcompanies.id = receptionform.exportcompany_id
        LEFT JOIN 
            crops ON crops.id = receptionform.crop_id;
    """)

    if rows1:
        data1 = pd.DataFrame(
            rows1, columns=['status', 'product_weight', 'qcp5_timestamp']
        )

        data1['year'] = pd.to_datetime(data1['qcp5_timestamp']).dt.year
        data1['product_weight'] = data1['product_weight'].astype(int)
        data1['month_number'] = pd.to_datetime(
            data1['qcp5_timestamp']).dt.month
        data1['month'] = data1['month_number'].apply(
            lambda x: calendar.month_abbr[x].upper()
        )

    if rows2:
        data2 = pd.DataFrame(
            rows2, columns=['country', 'clientcompany',
                            'product_weight', 'order_id', 'created_at']
        )

        data2['year'] = pd.to_datetime(data2['created_at']).dt.year
        data2['product_weight'] = data2['product_weight'].astype(int)
        data2['month_number'] = pd.to_datetime(data2['created_at']).dt.month
        data2['month'] = data2['month_number'].apply(
            lambda x: calendar.month_abbr[x].upper()
        )

    if rows3:
        data3 = pd.DataFrame(
            rows3, columns=['reception_id', 'reception_qtyremoved', 'qcp1_qtyremoved',
                            'qcp2_qtyremoved', 'qcp3_qtyremoved', 'qcp4_weight_rejected',
                            'qcp5_weight_rejected', 'exporter_name', 'cropname',
                            'reception_qtyrejected', 'created_at']
        )

        data3['year'] = pd.to_datetime(data3['created_at']).dt.year
        data3['month_number'] = pd.to_datetime(data3['created_at']).dt.month
        data3['month'] = data3['month_number'].apply(
            lambda x: calendar.month_abbr[x].upper()
        )

# Function to create CSV download link


def filedownload(df, filename="download.csv"):
    csv = df.to_csv(index=False)
    # strings <-> bytes conversions
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV File</a>'
    return href


# Sidebar
with st.sidebar:
    st.title('🥑 CICS Dashboard')

    year_list = sorted(list(set(data1['year']).union(
        data2['year'], data3['year'])), reverse=True)
    selected_year = st.selectbox('Select a year', year_list)

    status_list = list(data1['status'].unique())
    selected_status = st.multiselect(
        'Select status', status_list, default=status_list)

    country_list = list(data2['country'].unique())
    selected_countries = st.multiselect(
        'Select countries', country_list, default=country_list)

    crop_list = list(data3['cropname'].unique())
    selected_crops = st.multiselect(
        'Select crops', crop_list, default=crop_list)

    # Filtering data
filter_data1 = data1[(data1['year'] == selected_year) &
                     (data1['status'].isin(selected_status))]
filter_data2 = data2[(data2['year'] == selected_year) &
                     (data2['country'].isin(selected_countries))]
filter_data3 = data3[(data3['year'] == selected_year) &
                     (data3['cropname'].isin(selected_crops))]


# Calculate total weight for data1
total_weight = filter_data1['product_weight'].sum()
total_weight_str = f"{total_weight} Kg"
# show the status of the filter
status_weight = selected_status
# Filter columns for filter_data2
filter_data2 = filter_data2[[
    'country', 'clientcompany', 'product_weight']]

# Main Panel
col = st.columns((3, 5), gap='medium')

with col[0]:
    st.markdown('#### Total Product Weight')
    st.markdown(f"""
        <div style="background-color: #1f77b4; padding: 20px; border-radius: 5px; text-align: center;">
                <p></p>
            <h2 style="color: white; font-size: 50px;">{total_weight_str}</h2>
        </div>
    """, unsafe_allow_html=True)

    # Add extra space
    # Two line breaks for more space
    st.markdown("<br><br><br>", unsafe_allow_html=True)

    st.markdown('#### Destinations')
    # Create donut pie chart
    pie = alt.Chart(filter_data2).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="product_weight", type="quantitative"),
        color=alt.Color(field="clientcompany", type="nominal"),
        tooltip=['clientcompany', 'country', 'product_weight']
    ).properties(
        width=400,
        height=400
    )

    st.altair_chart(pie, use_container_width=True)

    st.markdown('#### Product Wastage')

with col[1]:
    st.markdown('#### Product Volumes Received Per Month')

    # Modified bar graph to show all statuses per month
    p = alt.Chart(filter_data1).mark_bar().encode(
        x=alt.X('month', title='Month'),
        y=alt.Y('sum(product_weight)', title='Total Product Weight'),
        color='status'  # Color encoding for different statuses
    ).properties(
        width=alt.Step(80)  # controls width of bar.
    )

    st.write(p)

    st.markdown('#### Product Exported Per Country Per Client')

    st.table(filter_data2[[
        'country', 'clientcompany', 'product_weight']].head())

    st.markdown('#### Product Wastage')
    reception_data_melted = pd.melt(
        filter_data3,
        id_vars=['reception_id', 'exporter_name', 'cropname', 'created_at'],
        value_vars=['reception_qtyremoved', 'qcp1_qtyremoved', 'qcp2_qtyremoved',
                    'qcp3_qtyremoved', 'qcp4_weight_rejected', 'qcp5_weight_rejected'],
        var_name='Stage',
        value_name='Quantity'
    )

    reception_data_melted['created_at'] = pd.to_datetime(
        reception_data_melted['created_at'])
    reception_data_melted['month'] = reception_data_melted['created_at'].dt.month.apply(
        lambda x: calendar.month_abbr[x].upper())

    reception_line_chart = alt.Chart(reception_data_melted).mark_line().encode(
        x='month:O',
        y='Quantity:Q',
        color='Stage:N',
        tooltip=['month:O', 'Quantity:Q', 'Stage:N']
    ).properties(
        width=600,
        height=400
    )

    st.altair_chart(reception_line_chart, use_container_width=True)

# Add download links at the bottom of the sidebar
st.sidebar.markdown("### Download Filtered Data")
st.sidebar.markdown(filedownload(
    filter_data1, "filtered_data1.csv"), unsafe_allow_html=True)
st.sidebar.markdown(filedownload(
    filter_data2, "filtered_data2.csv"), unsafe_allow_html=True)
st.sidebar.markdown(filedownload(
    filter_data3, "filtered_data3.csv"), unsafe_allow_html=True)


def close_connection():
    if server:
        server.stop()


if 'shutdown' not in st.session_state:
    st.session_state.shutdown = False

if st.session_state.shutdown:
    close_connection()

st.session_state.shutdown = True
