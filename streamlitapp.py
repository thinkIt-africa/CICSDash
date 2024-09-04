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
data4 = pd.DataFrame(columns=['traceability_code',
                     'cropname', 'netweight', 'created_at', 'district'])

# Check if the connection was successful
if conn:
    st.write("# CICS Dashboard")

    # Perform query using st.experimental_memo
    @st.cache_data(ttl=600)
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

    rows4 = run_query("""
        SELECT

            crops.cropname,
            receptionform.netweight,
            receptionform.created_at,
            districts.name AS district
        FROM
            receptionform
        JOIN
            farmers ON farmers.id = receptionform.supplier_id
        JOIN
            farmer_farms ON farmer_farms.farmer_id = receptionform.supplier_id
        JOIN
            crops ON farmer_farms.crop_id = crops.id
        JOIN
            modesoftransport ON modesoftransport.id = receptionform.modeoftransport_id
        JOIN
            districts ON districts.id = farmer_farms.district_id;
    """)

    rows5 = run_query("""
        SELECT
            countries.countryname, crops.cropname, order_crops.weightreceived,
            order_crops.weight, exportcompanies.expname AS exportcompanyname
        FROM orders
        LEFT JOIN exportcompanies ON exportcompanies.id = orders.exportcompany_id
        LEFT JOIN clients ON clients.id = orders.client_id
        LEFT JOIN countries ON countries.id = clients.country_id
        LEFT JOIN order_crops ON order_crops.order_id = orders.id
        LEFT JOIN crops ON crops.id = order_crops.crop_id;
    """)

    rows6 = run_query("""
        SELECT
            crops.cropname, order_crops.weightreceived, order_crops.weight,
            countries.countryname
        FROM orders
        LEFT JOIN exportcompanies ON exportcompanies.id = orders.exportcompany_id
        LEFT JOIN clients ON clients.id = orders.client_id
        LEFT JOIN countries ON countries.id = clients.country_id
        LEFT JOIN order_crops ON order_crops.order_id = orders.id
        LEFT JOIN crops ON crops.id = order_crops.crop_id;
    """)

    if rows1:
        data1 = pd.DataFrame(
            rows1, columns=['status', 'product_weight', 'qcp5_timestamp']
        )

        data1['year'] = pd.to_datetime(data1['qcp5_timestamp']).dt.year
        data1['product_weight'] = data1['product_weight'].fillna(
            0).astype(int)  # Fill NaN with 0 and convert to int
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
        data2['product_weight'] = data2['product_weight'].fillna(
            0).astype(int)  # Fill NaN with 0 and convert to int
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

    if rows4:
        data4 = pd.DataFrame(
            rows4, columns=['cropname',
                            'netweight', 'created_at', 'district']
        )

        data4['year'] = pd.to_datetime(data4['created_at']).dt.year
        data4['netweight'] = data4['netweight'].fillna(
            0).astype(int)

    if rows5:
        data5 = pd.DataFrame(
            rows5, columns=['countryname', 'cropname',
                            'weightreceived', 'weight', 'exportcompanyname']
        )

    if rows6:
        data6 = pd.DataFrame(
            rows6, columns=['cropname', 'weightreceived',
                            'weight', 'countryname']
        )
        # Fill NaN with 0 and convert to int

# Function to create CSV download link


def filedownload(df, filename="download.csv"):
    csv = df.to_csv(index=False)
    # strings <-> bytes conversions
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV File</a>'
    return href

# Function to combine dataframes and create a single CSV download link


def combined_filedownload(df1, df2, df3, df4, filename="combined_data.csv"):
    combined_df = pd.concat([df1, df2, df3, df4], ignore_index=True)
    csv = combined_df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download Combined CSV File</a>'
    return href


# Sidebar
with st.sidebar:
    st.title('🥑 CICS Dashboard')

    year_list = sorted(list(set(data1['year']).union(
        data2['year'], data3['year'], data4['year'])), reverse=True)
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

    district_list = list(data4['district'].unique())
    selected_districts = st.multiselect(
        'Select districts', district_list, default=district_list)

    # New filter for "Top Crop Exported by Country"
    top_crop_list = list(data6['cropname'].unique())
    selected_top_crops = st.multiselect(
        'Select Top Crops for Exported by Country', top_crop_list, default=top_crop_list)

    # New filter for "Top Export Companies"
    export_company_list = list(data5['exportcompanyname'].unique())
    selected_export_companies = st.multiselect(
        'Select TopExport Companies', export_company_list, default=export_company_list)

    # Filtering data
    filter_data1 = data1[(data1['year'] == selected_year) &
                         (data1['status'].isin(selected_status))]
    filter_data2 = data2[(data2['year'] == selected_year) &
                         (data2['country'].isin(selected_countries))]
    filter_data3 = data3[(data3['year'] == selected_year) &
                         (data3['cropname'].isin(selected_crops))]
    filter_data4 = data4[(data4['year'] == selected_year) &
                         (data4['district'].isin(selected_districts))]
    # Filtering Top Crop Export Summary by selected crops and countries
    filtered_crop_export_summary = data6[
        (data6['cropname'].isin(selected_top_crops)) &
        (data6['countryname'].isin(selected_countries))
    ].sort_values(by='weightreceived', ascending=False)

    # Filtering Top Export Companies by selected crops and companies
    filtered_top_exporters_agg = data5[
        (data5['cropname'].isin(selected_top_crops)) &
        (data5['countryname'].isin(selected_countries)) &
        (data5['exportcompanyname'].isin(selected_export_companies))
    ]

    st.markdown(combined_filedownload(filter_data1, filter_data2, filter_data3, filter_data4, ),
                unsafe_allow_html=True)

# Calculate total weight for data1
total_weight = filter_data1['product_weight'].sum()
total_weight_str = f"{total_weight:,.0f} Kg"
# show the status of the filter
status_weight = selected_status
# Filter columns for filter_data2
filter_data2 = filter_data2[[
    'country', 'clientcompany', 'product_weight']]
# Group by country and sum the product weights
country_weights = filter_data2.groupby('country')['product_weight'].sum()
# Get the top 5 countries with the highest product_weight
top_5_countries = country_weights.nlargest(5).index

# Filter the original dataframe to only include rows from these top 5 countries
filter_data2_top5 = filter_data2[filter_data2['country'].isin(top_5_countries)]

# Main Panel
col = st.columns((4, 5), gap='medium')

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

    st.markdown('#### Top 5 Country Destinations')
    # Create donut pie chart
    pie = alt.Chart(filter_data2_top5).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="product_weight", type="quantitative"),
        color=alt.Color(field="country", type="nominal"),
        tooltip=['country', 'clientcompany', 'product_weight']
    ).properties(
        width=400,
        height=400
    )

    st.altair_chart(pie, use_container_width=True)

    st.markdown('### Top Export Districts')

    # Aggregate the data by district and cropname, summing up the netweight for each group
    filter_data4_sorted = filter_data4.groupby(['district', 'cropname'])[
        'netweight'].sum().reset_index()

    # Sort the aggregated data by netweight in descending order
    filter_data4_sorted = filter_data4_sorted.sort_values(
        by='netweight', ascending=False)

    # Check if the DataFrame is not empty before finding the max value
    if not filter_data4_sorted.empty:
        max_netweight = max(filter_data4_sorted['netweight'])
    else:
        max_netweight = 0  # Default value if the DataFrame is empty

    st.dataframe(filter_data4_sorted,
                 column_order=("district", "cropname", "netweight"),
                 hide_index=True,
                 width=None,
                 column_config={
                     "cropname": st.column_config.TextColumn(
                         "Crop Name",
                     ),
                     "district": st.column_config.TextColumn(
                         "District",
                     ),
                     "netweight": st.column_config.ProgressColumn(
                         "Net Weight(Kg)",
                         format="%d",
                         min_value=0, max_value=20000,),
                 }
                 )

    st.markdown('### Top Export Companies')

    # Aggregate the total exported weight for export companies
    top_exporters_aggregated = data5.groupby(['exportcompanyname']).agg(
        total_weight_received=('weightreceived', 'sum'), total_weight=('weight', 'sum')).reset_index()

    top_exporters = top_exporters_aggregated.sort_values(
        by='total_weight_received', ascending=False)

    st.dataframe(top_exporters,
                 column_order=("exportcompanyname",
                               "total_weight_received"),
                 hide_index=True,
                 column_config={
                     "exportcompanyname": st.column_config.TextColumn("Export Company"),
                     "total_weight_received": st.column_config.ProgressColumn("Total Weight Received(Kg)", format="%d", min_value=0, max_value=50000,), })
    #  "total_weight_exported": st.column_config.ProgressColumn("Total Exported Weight", format="%d", min_value=0), })

with col[1]:
    st.markdown('#### Product Volumes Received Per Month')

    # Modified bar graph to show all statuses
    p = alt.Chart(filter_data1).mark_bar().encode(
        x=alt.X('month:O', sort=list(calendar.month_abbr)),
        y=alt.Y('product_weight:Q'),
        color='status:N',
        tooltip=['month', 'product_weight', 'status']
    ).properties(
        width=600,
        height=400
    )

    st.altair_chart(p, use_container_width=True)

    st.markdown('#### Product Wastage')

    # Exporters filter
    filter_data3 = filter_data3[[
        'exporter_name', 'reception_qtyremoved', 'qcp1_qtyremoved',
        'qcp2_qtyremoved', 'qcp3_qtyremoved', 'qcp4_weight_rejected',
        'qcp5_weight_rejected'
    ]]
    filter_data3 = filter_data3.melt(
        id_vars=['exporter_name'], var_name='Stage', value_name='Quantity')
    reception_data_melted = filter_data3.groupby(
        ['exporter_name', 'Stage']).sum().reset_index()
    reception_data_melted['year'] = selected_year
    reception_line_chart = alt.Chart(reception_data_melted).mark_line().encode(
        x='exporter_name:O',
        y='Quantity:Q',
        color='Stage:N',
        tooltip=['exporter_name', 'Quantity', 'Stage']
    ).properties(
        width=600,
        height=400
    )

    st.altair_chart(reception_line_chart, use_container_width=True)

    st.markdown('### Top Crop Exported')
    # Aggregate the total weight and weight received for each crop name and country
    crop_export_summary_aggregated = data6.groupby(['cropname', 'countryname']).agg(
        total_weight_received=('weightreceived', 'sum'), total_weight=('weight', 'sum')).reset_index()

    # Sort by total weight received, with the highest weight first
    crop_export_summary_sorted = crop_export_summary_aggregated.sort_values(
        by='total_weight_received', ascending=False)

    # Display the sorted data
    st.dataframe(crop_export_summary_sorted,
                 column_order=("cropname", "countryname",
                               "total_weight_received"),
                 hide_index=True,
                 use_container_width=True,
                 column_config={
                     "cropname": st.column_config.TextColumn("Crop Name"),
                     "countryname": st.column_config.TextColumn("Country"),
                     "total_weight_received": st.column_config.ProgressColumn("Weight for Export(Kg)", format="%d", min_value=0, max_value=20000,)

                 }
                 )


# Close connection when the app is shut down


def close_connection():
    if server:
        server.stop()


# if st.button("Shutdown Connection"):
#     close_connection()
#     st.session_state.shutdown = True
