import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import numpy as np
import os
import io

# Page configuration
st.set_page_config(
    page_title="Northwind Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - UPDATED for metric cards
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 0.8rem;  /* Reduced from 1.2rem */
        border-radius: 8px;  /* Reduced from 10px */
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);  /* Reduced shadow */
        text-align: center;
        border: 1px solid #e0e0e0;  /* Thinner border */
        min-height: 140px;  /* Reduced from 160px */
        display: flex;
        flex-direction: column;
        justify-content: center;
        position: relative;
    }
    .metric-label {
        font-size: 1.2rem;
        color: #495057;
        font-weight: 700;
        margin-bottom: 0.8rem;
        padding: 8px 12px;
        background-color: #f8f9fa;
        border-radius: 6px;
        border: 1px solid #dee2e6;
    }
    .metric-value {
        font-size: 2.5rem;  /* Reduced from 3rem */
        font-weight: bold;
        color: #2c3e50;
        margin: 3px 0;  /* Reduced margin */
        line-height: 1;
    }
    .metric-subtext {
        font-size: 0.9rem;
        color: #6c757d;
        margin-top: 8px;
    }
    .alert-text {
        color: #dc3545;
        font-weight: 600;
        font-size: 1rem;
        margin-top: 8px;
        padding: 4px 10px;
        background-color: rgba(220, 53, 69, 0.08);
        border-radius: 4px;
        border-left: 3px solid #dc3545;
    }
    .success-text {
        color: #198754;
        font-weight: 600;
        font-size: 1rem;
        margin-top: 8px;
        padding: 4px 10px;
        background-color: rgba(25, 135, 84, 0.08);
        border-radius: 4px;
        border-left: 3px solid #198754;
    }
    .stButton>button {
        width: 100%;
        background-color: #0d6efd;
        color: white;
        font-weight: bold;
        border: none;
        padding: 0.75rem;
        border-radius: 8px;
        font-size: 1.1rem;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #0b5ed7;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(13, 110, 253, 0.2);
    }
    .section-title {
        font-size: 1.8rem;
        color: #2c3e50;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
    .dataframe {
        border: 1px solid #dee2e6;
        border-radius: 8px;
        overflow: hidden;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f8f9fa;
        border-radius: 4px 4px 0px 0px;
        gap: 1rem;
        padding: 10px 16px;
        font-weight: 600;
        color: #000000 !important;  
    }
</style>
""", unsafe_allow_html=True)


# Database connection function
@st.cache_resource
def connect_to_dw():
    """Connect to data warehouse"""
    try:
        from DatabaseConfig import connect_data_warehouse
        conn = connect_data_warehouse()
        return conn
    except Exception as e:
        st.error(f"Error connecting to DW: {e}")
        return None


# Load data from DW
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_dashboard_data():
    """Load data for dashboard from Data Warehouse"""
    conn = connect_to_dw()
    if conn is None:
        return pd.DataFrame()

    try:
        # Query for dashboard data
        query = """
        SELECT 
            fo.OrderID,
            fo.OrderDate,
            fo.ShippedDate,
            fo.TotalAmount,
            fo.IsDelivered,
            fo.SourceSystem,
            dc.CompanyName as CustomerName,
            de.FirstName + ' ' + de.LastName as EmployeeName
        FROM FactOrders fo
        LEFT JOIN DimCustomer dc ON fo.CustomerKey = dc.CustomerKey
        LEFT JOIN DimEmployee de ON fo.EmployeeKey = de.EmployeeKey
        WHERE fo.OrderDate IS NOT NULL
        ORDER BY fo.OrderDate DESC
        """

        df = pd.read_sql(query, conn)
        conn.close()

        # Process dates - SAFELY
        if 'OrderDate' in df.columns:
            df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
            df['Year'] = df['OrderDate'].dt.year
            df['Month'] = df['OrderDate'].dt.month
            df['YearMonth'] = df['OrderDate'].dt.strftime('%Y-%m')

        if 'ShippedDate' in df.columns:
            df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

        # Calculate delivery status
        df['Status'] = df['IsDelivered'].apply(lambda x: 'Livrée' if x == 1 else 'Non Livrée')

        # Fill NaN values
        df['CustomerName'] = df['CustomerName'].fillna('Unknown Customer')
        df['EmployeeName'] = df['EmployeeName'].fillna('Unknown Employee')

        return df

    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")
        return pd.DataFrame()


# Function to run ETL
def run_etl():
    """Run ETL process"""
    try:
        from etl import etl
        etl_processor = etl()
        etl_processor.run_full_etl()
        return True, "✅ ETL completed successfully!"
    except Exception as e:
        return False, f"❌ ETL Error: {str(e)}"


# Initialize session state for data
if 'data' not in st.session_state:
    st.session_state.data = load_dashboard_data()

if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# Main dashboard layout
st.markdown('<h1 class="main-header">Contrôle Northwind</h1>', unsafe_allow_html=True)

# ETL Button
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("ACTUALISER", key="etl_button"):
        with st.spinner("Running ETL process..."):
            success, message = run_etl()
            if success:
                st.success(message)
                # Clear cache and reload data
                st.cache_data.clear()
                st.session_state.data = load_dashboard_data()
                st.session_state.last_refresh = datetime.now()
                st.rerun()
            else:
                st.error(message)

# Show last refresh time
st.caption(f"*Dernière actualisation: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}*")

# Key Metrics - UPDATED to match your image layout
st.markdown('<h2 class="section-title">Indicateurs Clés</h2>', unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

df = st.session_state.data

if not df.empty:
    total_orders = len(df)
    delivered_orders = df['IsDelivered'].sum() if 'IsDelivered' in df.columns else 0
    not_delivered = total_orders - delivered_orders
    delivery_rate = (delivered_orders / total_orders * 100) if total_orders > 0 else 0

    # Metric 1: Total Commandes
    with col1:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-label">Total Commandes</div>
            <div class="metric-value">{total_orders:,}</div>
            <div class="metric-subtext">Commandes totales</div>
        </div>
        ''', unsafe_allow_html=True)

    # Metric 2: Livrées
    with col2:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-label">Livrées</div>
            <div class="metric-value">{delivered_orders:,}</div>
            <div class="alert-text">Non Livrées {not_delivered} Alert</div>
        </div>
        ''', unsafe_allow_html=True)

    # Metric 3: Taux de Livraison
    with col3:
        st.markdown(f'''
        <div class="metric-card">
            <div class="metric-label">Taux de Livraison</div>
            <div class="metric-value" style="color: #198754;">{delivery_rate:.1f}%</div>
            <div class="success-text">Performance optimale</div>
        </div>
        ''', unsafe_allow_html=True)
else:
    st.warning("No data available. Please run ETL first.")

# Filters in sidebar
st.sidebar.markdown("## Filtres")

if not df.empty:
    # Year filter
    years = sorted(df['Year'].dropna().unique())
    selected_years = st.sidebar.multiselect(
        "Année:",
        options=years,
        default=years[:min(3, len(years))]
    )

    # Customer filter
    customers = sorted(df['CustomerName'].dropna().unique())
    selected_customers = st.sidebar.multiselect(
        "Client:",
        options=customers,
        default=customers[:min(5, len(customers))] if len(customers) > 0 else []
    )

    # Employee filter
    employees = sorted(df['EmployeeName'].dropna().unique())
    selected_employees = st.sidebar.multiselect(
        "Employé:",
        options=employees,
        default=employees[:min(5, len(employees))] if len(employees) > 0 else []
    )

    # Status filter
    status_options = ['Tous', 'Livrées', 'Non Livrées']
    selected_status = st.sidebar.radio(
        "Statut:",
        options=status_options
    )

    # Graph type selection
    graph_types = ['Scatter 3D', 'Surface 3D', 'Bubble 3D']
    selected_graph = st.sidebar.selectbox(
        "Type de graphique 3D:",
        options=graph_types
    )

    # Apply filters
    filtered_df = df.copy()

    if selected_years:
        filtered_df = filtered_df[filtered_df['Year'].isin(selected_years)]

    if selected_customers:
        filtered_df = filtered_df[filtered_df['CustomerName'].isin(selected_customers)]

    if selected_employees:
        filtered_df = filtered_df[filtered_df['EmployeeName'].isin(selected_employees)]

    if selected_status == 'Livrées':
        filtered_df = filtered_df[filtered_df['IsDelivered'] == 1]
    elif selected_status == 'Non Livrées':
        filtered_df = filtered_df[filtered_df['IsDelivered'] == 0]

    # Summary stats in sidebar
    st.sidebar.markdown("## Résumé")
    if not filtered_df.empty:
        total_filtered = len(filtered_df)
        delivered_filtered = filtered_df['IsDelivered'].sum()
        not_delivered_filtered = total_filtered - delivered_filtered
        revenue_filtered = filtered_df['TotalAmount'].sum()
        avg_order_filtered = revenue_filtered / total_filtered if total_filtered > 0 else 0

        st.sidebar.metric("Commandes filtrées", f"{total_filtered:,}")
        st.sidebar.metric("Livrées", f"{delivered_filtered:,}")
        st.sidebar.metric("Non livrées", f"{not_delivered_filtered:,}")
        st.sidebar.metric("Revenu total", f"${revenue_filtered:,.0f}")
        st.sidebar.metric("Moyenne/commande", f"${avg_order_filtered:,.0f}")
        st.sidebar.metric("Clients uniques", filtered_df['CustomerName'].nunique())
        st.sidebar.metric("Employés uniques", filtered_df['EmployeeName'].nunique())
else:
    filtered_df = pd.DataFrame()
    selected_graph = 'Scatter 3D'

# Main content area
tab1, tab2, tab3 = st.tabs(["📊 Graphique 3D", "📈 Évolution", "📋 Données"])

with tab1:
    st.markdown('<h2 class="section-title">Commandes par Client et Employé (3D)</h2>', unsafe_allow_html=True)

    if not filtered_df.empty:
        # Remove rows with null CustomerName or EmployeeName for 3D plot
        plot_df = filtered_df.dropna(subset=['CustomerName', 'EmployeeName'])

        if not plot_df.empty:
            # Group data for 3D visualization
            grouped = plot_df.groupby(['CustomerName', 'EmployeeName']).agg({
                'OrderID': 'count',
                'TotalAmount': 'sum',
                'IsDelivered': 'mean'
            }).reset_index()

            grouped.columns = ['CustomerName', 'EmployeeName', 'OrderCount', 'TotalRevenue', 'DeliveryRate']

            if selected_graph == 'Scatter 3D':
                fig = go.Figure(data=[go.Scatter3d(
                    x=grouped['CustomerName'],
                    y=grouped['EmployeeName'],
                    z=grouped['OrderCount'],
                    mode='markers',
                    marker=dict(
                        size=grouped['TotalRevenue'] / grouped['TotalRevenue'].max() * 30 + 5,
                        color=grouped['DeliveryRate'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title="Taux Livraison")
                    ),
                    text=[
                        f"Client: {c}<br>Employé: {e}<br>Commandes: {oc}<br>Revenu: ${tr:,.0f}<br>Taux Livraison: {dr * 100:.1f}%"
                        for c, e, oc, tr, dr in zip(
                            grouped['CustomerName'], grouped['EmployeeName'],
                            grouped['OrderCount'], grouped['TotalRevenue'], grouped['DeliveryRate']
                        )
                    ],
                    hoverinfo='text'
                )])

                fig.update_layout(
                    scene=dict(
                        xaxis_title="Client",
                        yaxis_title="Employé",
                        zaxis_title="Nombre Commandes"
                    ),
                    title="Commandes par Client et Employé (3D Scatter)",
                    height=600
                )

            elif selected_graph == 'Surface 3D':
                # Create pivot table for surface plot
                pivot = plot_df.pivot_table(
                    values='OrderID',
                    index='CustomerName',
                    columns='EmployeeName',
                    aggfunc='count',
                    fill_value=0
                )

                # Get the data for surface
                customers = pivot.index.tolist()
                employees = pivot.columns.tolist()
                z_data = pivot.values

                # Check if we have enough data for surface plot
                if len(customers) >= 2 and len(employees) >= 2:
                    fig = go.Figure(data=[go.Surface(
                        x=employees,
                        y=customers,
                        z=z_data,
                        colorscale='Viridis',
                        contours={
                            "z": {"show": True, "usecolormap": True, "highlightcolor": "limegreen",
                                  "project": {"z": True}}
                        }
                    )])

                    fig.update_layout(
                        scene=dict(
                            xaxis_title="Employé",
                            yaxis_title="Client",
                            zaxis_title="Nombre Commandes"
                        ),
                        title="Commandes par Client et Employé (3D Surface)",
                        height=600
                    )
                else:
                    st.info("Not enough data for 3D Surface plot. Need at least 2 customers and 2 employees.")
                    fig = go.Figure().update_layout(
                        title="Not enough data for 3D Surface plot"
                    )

            elif selected_graph == 'Bubble 3D':
                fig = go.Figure(data=[go.Scatter3d(
                    x=grouped['CustomerName'],
                    y=grouped['EmployeeName'],
                    z=grouped['OrderCount'],
                    mode='markers+text',
                    marker=dict(
                        size=grouped['OrderCount'] * 2,
                        color=grouped['TotalRevenue'],
                        colorscale='Rainbow',
                        showscale=True,
                        colorbar=dict(title="Revenu Total")
                    ),
                    text=grouped['OrderCount'].astype(str),
                    textposition="middle center",
                    hovertext=[
                        f"Client: {c}<br>Employé: {e}<br>Commandes: {oc}<br>Revenu: ${tr:,.0f}"
                        for c, e, oc, tr in zip(
                            grouped['CustomerName'], grouped['EmployeeName'],
                            grouped['OrderCount'], grouped['TotalRevenue']
                        )
                    ],
                    hoverinfo='text'
                )])

                fig.update_layout(
                    scene=dict(
                        xaxis_title="Client",
                        yaxis_title="Employé",
                        zaxis_title="Nombre Commandes"
                    ),
                    title="Commandes par Client et Employé (Bubble 3D)",
                    height=600
                )

            st.plotly_chart(fig, use_container_width=True)

            # Show data summary
            with st.expander("📊 Résumé des données 3D"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Clients affichés", grouped['CustomerName'].nunique())
                with col2:
                    st.metric("Employés affichés", grouped['EmployeeName'].nunique())
                with col3:
                    st.metric("Relations totales", len(grouped))
        else:
            st.info("No data with complete customer and employee information for 3D plot.")
    else:
        st.warning("No data available for visualization. Please apply different filters.")

with tab2:
    st.markdown('<h2 class="section-title">Évolution des Commandes (Livrées vs Non Livrées)</h2>', unsafe_allow_html=True)

    if not filtered_df.empty:
        # Group by YearMonth and status
        if 'YearMonth' not in filtered_df.columns:
            filtered_df['YearMonth'] = filtered_df['OrderDate'].dt.strftime('%Y-%m')

        grouped = filtered_df.groupby(['YearMonth', 'Status']).size().reset_index(name='Count')

        fig = px.bar(
            grouped,
            x='YearMonth',
            y='Count',
            color='Status',
            barmode='group',
            color_discrete_map={'Livrée': '#198754', 'Non Livrée': '#dc3545'},
            title="Évolution des Commandes (Livrées vs Non Livrées)"
        )

        fig.update_layout(
            xaxis_title="Mois",
            yaxis_title="Nombre de Commandes",
            hovermode='x unified',
            legend_title="Statut"
        )

        st.plotly_chart(fig, use_container_width=True)

        # Line chart alternative
        fig2 = px.line(
            grouped,
            x='YearMonth',
            y='Count',
            color='Status',
            markers=True,
            title="Tendance des Commandes (Ligne)"
        )

        fig2.update_layout(
            xaxis_title="Mois",
            yaxis_title="Nombre de Commandes",
            hovermode='x unified'
        )

        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data available for evolution chart.")

with tab3:
    st.markdown('<h2 class="section-title">Détails des Commandes</h2>', unsafe_allow_html=True)

    if not filtered_df.empty:
        # Format data for display
        display_df = filtered_df.copy()

        # Format date columns SAFELY
        if 'OrderDate' in display_df.columns:
            display_df['OrderDate'] = display_df['OrderDate'].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
            )

        if 'ShippedDate' in display_df.columns:
            display_df['ShippedDate'] = display_df['ShippedDate'].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
            )

        # Format currency
        if 'TotalAmount' in display_df.columns:
            display_df['TotalAmount'] = display_df['TotalAmount'].apply(
                lambda x: f"${float(x):,.2f}" if pd.notna(x) else "$0.00"
            )

        # Select columns to display
        cols_to_display = ['OrderID', 'OrderDate', 'ShippedDate', 'CustomerName',
                           'EmployeeName', 'TotalAmount', 'Status', 'SourceSystem']
        available_cols = [col for col in cols_to_display if col in display_df.columns]

        display_df = display_df[available_cols]

        # Show data table - FIXED Streamlit warning
        st.dataframe(
            display_df,
            width='stretch',
            hide_index=True,
            column_config={
                "OrderID": st.column_config.NumberColumn("ID", format="%d"),
                "OrderDate": "Date Commande",
                "ShippedDate": "Date Livraison",
                "CustomerName": "Client",
                "EmployeeName": "Employé",
                "TotalAmount": "Montant",
                "Status": "Statut",
                "SourceSystem": "Source"
            }
        )

        # Export options - FIXED Excel export
        col1, col2 = st.columns(2)
        with col1:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Télécharger CSV",
                data=csv,
                file_name="northwind_orders.csv",
                mime="text/csv",
                use_container_width=True
            )
        with col2:
            # FIXED: Correct Excel export for newer pandas
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                display_df.to_excel(writer, index=False, sheet_name='Orders')
                writer._save()  # Use _save() instead of save()

            st.download_button(
                label="📥 Télécharger Excel",
                data=buffer.getvalue(),
                file_name="northwind_orders.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:
        st.warning("No data available to display.")

# Footer
st.markdown("---")
st.caption("Dashboard Northwind - Système de Contrôle des Commandes | © 2025")