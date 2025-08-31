# -----------------------------
# PhonePe Pulse Dashboard with Sidebar Navigation (Updated: Insurance fixes + Scenarios 4 & 5)
# -----------------------------
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import streamlit as slt
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any
import requests

slt.set_page_config(layout="wide", page_title="PhonePe Dashboard")

# -----------------------------
# DB Connection
# -----------------------------
def get_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("RENDER_DB_HOST"),
        port=os.getenv("RENDER_DB_PORT"),
        dbname=os.getenv("RENDER_DB_NAME"),
        user=os.getenv("RENDER_DB_USER"),
        password=os.getenv("RENDER_DB_PASSWORD")
    )

def fetch_df(sql: str, params: tuple, cols: List[str]) -> pd.DataFrame:
    """
    Basic fetch wrapper used across app. On DB errors it shows an slt.error and returns empty DataFrame.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        slt.error(f"Database error: {e}")
        return pd.DataFrame(columns=cols)

def try_query_variants(variants: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Try multiple (sql, params, cols) variants in order and return the first non-empty DataFrame.
    Each variant is a dict: {'sql': str, 'params': tuple|None, 'cols': list}
    Returns empty DataFrame with 'cols' of first variant if none succeeded.
    """
    if not variants:
        return pd.DataFrame()
    first_cols = variants[0].get("cols", [])
    for v in variants:
        try:
            conn = get_connection()
            cur = conn.cursor()
            if v.get("params") is None:
                cur.execute(v["sql"])
            else:
                cur.execute(v["sql"], v["params"])
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if rows:
                return pd.DataFrame(rows, columns=v["cols"])
        except Exception:
            # silently try next variant (don't spam user with DB errors here)
            continue
    return pd.DataFrame(columns=first_cols)

# -----------------------------
# Cache GeoJSON
# -----------------------------
@slt.cache_data(ttl=60*60)
def load_india_geojson():
    url = (
        "https://gist.githubusercontent.com/jbrobst/"
        "56c13bbbf9d97d187fea01ca62ea5112/raw/"
        "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

QMAP = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
def year_options():
    return [2018, 2019, 2020, 2021, 2022, 2023, 2024]

# -----------------------------
# Queries (for Home)
# -----------------------------
def query_state_transactions_all(year: int, quarter: int) -> pd.DataFrame:
    sql = """
        SELECT state,
               SUM(transaction_amount) AS total_transactions,
               AVG(transaction_amount) AS avg_transactions
        FROM agg_trans
        WHERE year::INT = %s AND quarter::INT = %s
        GROUP BY state ORDER BY state;
    """
    return fetch_df(sql, (year, quarter), ["State", "Total_Transactions", "Avg_Transactions"])

def query_state_users_all(year: int, quarter: int) -> pd.DataFrame:
    sql = """
        SELECT state,
               SUM(registered_users) AS registered_users
        FROM map_user
        WHERE year::INT = %s AND quarter::INT = %s
        GROUP BY state ORDER BY state;
    """
    return fetch_df(sql, (year, quarter), ["State", "Registered_Users"])

# Top 10 Queries
def top10_states_transactions(year, q):
    sql = """SELECT state, SUM(transaction_amount) AS total
             FROM agg_trans WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state ORDER BY total DESC LIMIT 10;"""
    return fetch_df(sql, (year, q), ["State","Total_Transactions"])

def top10_districts_transactions(year, q):
    sql = """SELECT state,district, SUM(transaction_amount) AS total
             FROM map_trans WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state,district ORDER BY total DESC LIMIT 10;"""
    return fetch_df(sql, (year,q), ["State","District","Total_Transactions"])

def top10_pincodes_transactions(year,q):
    sql = """SELECT state,pincode, SUM(transaction_amount) AS total
             FROM top_trans WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state,pincode ORDER BY total DESC LIMIT 10;"""
    return fetch_df(sql,(year,q),["State","Pincode","Total_Transactions"])

def top10_states_users(year,q):
    sql = """SELECT state, SUM(registered_users) AS users
             FROM map_user WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state ORDER BY users DESC LIMIT 10;"""
    return fetch_df(sql,(year,q),["State","Registered_Users"])

def top10_districts_users(year,q):
    sql = """SELECT state,district,SUM(registered_users) AS users
             FROM map_user WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state,district ORDER BY users DESC LIMIT 10;"""
    return fetch_df(sql,(year,q),["State","District","Registered_Users"])

def top10_pincodes_users(year,q):
    sql = """SELECT state,pincode,SUM(registered_users) AS users
             FROM top_user WHERE year::INT=%s AND quarter::INT=%s
             GROUP BY state,pincode ORDER BY users DESC LIMIT 10;"""
    return fetch_df(sql,(year,q),["State","Pincode","Registered_Users"])

# -----------------------------
# Queries (for Analysis Page)
# -----------------------------
def device_brand_usage(year: int, quarter: int) -> pd.DataFrame:
    sql = """
        SELECT user_brand, SUM(user_count) AS total_users, AVG(user_percentage) AS avg_share
        FROM agg_user
        WHERE year::INT = %s AND quarter::INT = %s
        GROUP BY user_brand
        ORDER BY total_users DESC;
    """
    return fetch_df(sql, (year, quarter), ["Brand", "Total_Users", "Avg_Share"])

def device_brand_by_state(year: int, quarter: int) -> pd.DataFrame:
    sql = """
        SELECT state, user_brand, SUM(user_count) AS total_users
        FROM agg_user
        WHERE year::INT = %s AND quarter::INT = %s
        GROUP BY state, user_brand;
    """
    return fetch_df(sql, (year, quarter), ["State", "Brand", "Total_Users"])

def app_opens_by_state(year: int, quarter: int) -> pd.DataFrame:
    sql = """
        SELECT state, SUM(CAST(app_opens AS BIGINT)) AS total_opens
        FROM map_user
        WHERE year::INT = %s AND quarter::INT = %s
        GROUP BY state ORDER BY state;
    """
    return fetch_df(sql, (year, quarter), ["State", "App_Opens"])

# -----------------------------
# Insurance-specific Queries (Scenario 3) - robust variants
# -----------------------------
def get_transaction_types() -> List[str]:
    """Try to fetch distinct transaction type names from known tables. Returns list or empty list."""
    variants = [
        {"sql":"SELECT DISTINCT transaction_type FROM agg_trans ORDER BY transaction_type;", "params": None, "cols":["Transaction_Type"]},
        {"sql":"SELECT DISTINCT transaction_type FROM map_trans ORDER BY transaction_type;", "params": None, "cols":["Transaction_Type"]}
    ]
    df = try_query_variants(variants)
    if df.empty:
        return []
    return [str(x) for x in df["Transaction_Type"].dropna().tolist()]

def insurance_state_summary(year: int, quarter: int, tx_type: str) -> pd.DataFrame:
    """
    Try multiple query variants:
      1) agg_trans filtered by transaction_type
      2) agg_insurance (if present)
    Returns first non-empty DataFrame.
    """
    variants = [
        {
            "sql": """
                SELECT state,
                       SUM(transaction_amount) AS total_premium,
                       SUM(COALESCE(transaction_count,0)) AS policies_sold
                FROM agg_trans
                WHERE year::INT=%s AND quarter::INT=%s AND LOWER(transaction_type)=LOWER(%s)
                GROUP BY state
                ORDER BY total_premium DESC;
            """,
            "params": (year, quarter, tx_type),
            "cols": ["State","Total_Premium","Policies_Sold"]
        },
        {
            "sql": """
                SELECT state,
                       SUM(transaction_amount) AS total_premium,
                       COUNT(*) AS policies_sold
                FROM agg_insurance
                WHERE year::INT=%s AND quarter::INT=%s
                GROUP BY state
                ORDER BY total_premium DESC;
            """,
            "params": (year, quarter),
            "cols": ["State","Total_Premium","Policies_Sold"]
        }
    ]
    return try_query_variants(variants)

def insurance_top_districts(year: int, quarter: int, tx_type: str) -> pd.DataFrame:
    variants = [
        {
            "sql": """
                SELECT state, district,
                       SUM(transaction_amount) AS total_premium,
                       SUM(COALESCE(transaction_count,0)) AS policies_sold
                FROM map_trans
                WHERE year::INT=%s AND quarter::INT=%s AND LOWER(transaction_type)=LOWER(%s)
                GROUP BY state, district
                ORDER BY total_premium DESC
                LIMIT 10;
            """,
            "params": (year, quarter, tx_type),
            "cols": ["State","District","Total_Premium","Policies_Sold"]
        },
        {
            "sql": """
                SELECT state, district, SUM(transaction_amount) AS total_premium, COUNT(*) AS policies_sold
                FROM map_insurance
                WHERE year::INT=%s AND quarter::INT=%s
                GROUP BY state, district
                ORDER BY total_premium DESC
                LIMIT 10;
            """,
            "params": (year, quarter),
            "cols": ["State","District","Total_Premium","Policies_Sold"]
        }
    ]
    return try_query_variants(variants)

def insurance_quarterly_trend(year: int, tx_type: str) -> pd.DataFrame:
    variants = [
        {
            "sql": """
                SELECT quarter::INT AS quarter,
                       SUM(transaction_amount) AS total_premium,
                       SUM(COALESCE(transaction_count,0)) AS policies_sold
                FROM agg_trans
                WHERE year::INT=%s AND LOWER(transaction_type)=LOWER(%s)
                GROUP BY quarter
                ORDER BY quarter;
            """,
            "params": (year, tx_type),
            "cols": ["Quarter","Total_Premium","Policies_Sold"]
        },
        {
            "sql": """
                SELECT quarter::INT AS quarter,
                       SUM(transaction_amount) AS total_premium,
                       COUNT(*) AS policies_sold
                FROM agg_insurance
                WHERE year::INT=%s
                GROUP BY quarter
                ORDER BY quarter;
            """,
            "params": (year,),
            "cols": ["Quarter","Total_Premium","Policies_Sold"]
        }
    ]
    return try_query_variants(variants)

# -----------------------------
# PAGES
# -----------------------------
def home_page():
    slt.title("PhonePe Pulse Home Page")
    controls_col1, controls_col2, controls_col3 = slt.columns([2, 1, 1])
    with controls_col1:
        metric_choice = slt.selectbox("Select Metric", ["Transactions", "Users"])
    with controls_col2:
        year = slt.selectbox("Year", year_options())
    with controls_col3:
        q_label = slt.selectbox("Quarter", ["Q1", "Q2", "Q3", "Q4"])
    q_num = QMAP[q_label]

    india_geo = load_india_geojson()
    map_col, _ = slt.columns([2.5, 0.5])

    with map_col:
        if metric_choice == "Transactions":
            df_map = query_state_transactions_all(year, q_num)
            if df_map.empty:
                slt.warning("No transaction data for selection.")
            else:
                fig = px.choropleth(
                    df_map, geojson=india_geo,
                    featureidkey="properties.ST_NM",
                    locations="State", color="Total_Transactions",
                    hover_data=["Avg_Transactions","Total_Transactions"],
                    color_continuous_scale="Viridis",
                    title=f"Total Transactions — {year} {q_label}"
                )
                fig.update_geos(center=dict(lat=22.0, lon=80.0),
                                projection_scale=4.0, visible=False)
                slt.plotly_chart(fig, use_container_width=True)
        else:
            df_map = query_state_users_all(year, q_num)
            if df_map.empty:
                slt.warning("No user data for selection.")
            else:
                fig = px.choropleth(
                    df_map, geojson=india_geo,
                    featureidkey="properties.ST_NM",
                    locations="State", color="Registered_Users",
                    hover_data=["Registered_Users"],
                    color_continuous_scale="Blues",
                    title=f"Registered Users — {year} {q_label}"
                )
                fig.update_geos(center=dict(lat=22.0, lon=80.0),
                                projection_scale=4.0, visible=False)
                slt.plotly_chart(fig, use_container_width=True)

    slt.markdown("---")
    slt.markdown(f"### Top 10 (by {metric_choice}) — {year} {q_label}")
    if metric_choice=="Transactions":
        slt.subheader("Top 10 States"); slt.dataframe(top10_states_transactions(year,q_num))
        slt.subheader("Top 10 Districts"); slt.dataframe(top10_districts_transactions(year,q_num))
        slt.subheader("Top 10 Pincodes"); slt.dataframe(top10_pincodes_transactions(year,q_num))
    else:
        slt.subheader("Top 10 States"); slt.dataframe(top10_states_users(year,q_num))
        slt.subheader("Top 10 Districts"); slt.dataframe(top10_districts_users(year,q_num))
        slt.subheader("Top 10 Pincodes"); slt.dataframe(top10_pincodes_users(year,q_num))

def analysis_page():
    slt.title("📊 Analysis Dashboard")

    # Scenario dropdown (added the 5th scenario here)
    scenario = slt.selectbox(
        "Choose Analysis Scenario",
        [
            "Device Brand Analysis — User Engagement & App Performance",
            "Transaction Analysis for Market Expansion",
            "Insurance Engagement Analysis",
            "Transaction Analysis Across States and Districts",
            "User Registration Analysis"
        ]
    )

    # Common controls
    col1, col2 = slt.columns([1, 1])
    with col1:
        year = slt.selectbox("Year", year_options(), key="ana_year")
    with col2:
        q_label = slt.selectbox("Quarter", ["Q1","Q2","Q3","Q4"], key="ana_q")
    q_num = QMAP[q_label]

    # ===================================================
    # Scenario 1: Device Dominance and User Engagement Analysis
    # ===================================================
    if scenario == "Device Brand Analysis — User Engagement & App Performance":
        df_brand = device_brand_usage(year, q_num)
        df_state_brand = device_brand_by_state(year, q_num)
        df_opens = app_opens_by_state(year, q_num).sort_values("State")

        # Bar chart — Top Device Brands
        if not df_brand.empty:
            fig1 = px.bar(df_brand, x="Brand", y="Total_Users", color="Brand",
                          title="Top Device Brands by Registered Users",
                          text="Total_Users", height=500)
            fig1.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig1.update_layout(xaxis_title="Brand", yaxis_title="Users")
            slt.plotly_chart(fig1, use_container_width=True)

            # Donut chart — Market Share
            fig2 = go.Figure(go.Pie(
                labels=df_brand["Brand"],
                values=df_brand["Total_Users"],
                hole=0.5,
                pull=[0.05]*len(df_brand),
                textinfo="label+percent",
                hoverinfo="label+value+percent"
            ))
            fig2.update_layout(title="📱 Device Brand Market Share", height=500)
            slt.plotly_chart(fig2, use_container_width=True)

        # Line chart — App Opens by State
        if not df_opens.empty:
            fig3 = px.line(df_opens, x="State", y="App_Opens",
                           title="📈 App Opens by State", markers=True, height=500)
            fig3.update_layout(xaxis=dict(tickangle=45))
            slt.plotly_chart(fig3, use_container_width=True)

        # Pie Chart — Device Brand usage by State
        if not df_state_brand.empty:
            states = df_state_brand["State"].unique()
            selected_state = slt.selectbox("Select State for Device Brand Usage", states)

            df_state_filtered = df_state_brand[df_state_brand["State"] == selected_state]

            fig4 = go.Figure(go.Pie(
                labels=df_state_filtered["Brand"],
                values=df_state_filtered["Total_Users"],
                hole=0.4,
                textinfo="label+percent",
                hoverinfo="label+value+percent"
            ))
            fig4.update_layout(
                title=f"📱 Device Brand Usage in {selected_state}",
                height=600
            )
            slt.plotly_chart(fig4, use_container_width=True)

    # ===================================================
    # Scenario 2: Transaction Analysis for Market Expansion
    # ===================================================
    elif scenario == "Transaction Analysis for Market Expansion":
        df_trans = query_state_transactions_all(year, q_num)
        df_top_states = top10_states_transactions(year, q_num)
        df_top_districts = top10_districts_transactions(year, q_num)
        df_top_pincodes = top10_pincodes_transactions(year, q_num)

        if df_trans.empty:
            slt.warning("No transaction data available for selection.")
        else:
            india_geo = load_india_geojson()

            # Choropleth — Transactions across States
            fig1 = px.choropleth(
                df_trans, geojson=india_geo,
                featureidkey="properties.ST_NM",
                locations="State", color="Total_Transactions",
                hover_data=["Avg_Transactions", "Total_Transactions"],
                color_continuous_scale="Viridis",
                title=f"🗺️ Transaction Intensity Across States — {year} {q_label}"
            )
            fig1.update_geos(center=dict(lat=22.0, lon=80.0),
                             projection_scale=4.0, visible=False)
            slt.plotly_chart(fig1, use_container_width=True)

            # Top 10 States by Transactions
            if not df_top_states.empty:
                # ensure numeric
                df_top_states["Total_Transactions"] = pd.to_numeric(df_top_states["Total_Transactions"], errors='coerce').fillna(0)
                fig2 = px.bar(df_top_states, x="State", y="Total_Transactions",
                              color="Total_Transactions", text="Total_Transactions",
                              title="🏆 Top 10 States by Transactions")
                fig2.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                slt.plotly_chart(fig2, use_container_width=True)

            # Top 10 Districts by Transactions
            if not df_top_districts.empty:
                df_top_districts["Total_Transactions"] = pd.to_numeric(df_top_districts["Total_Transactions"], errors='coerce').fillna(0)
                fig3 = px.bar(df_top_districts, x="District", y="Total_Transactions",
                              color="State", text="Total_Transactions",
                              title="🏆 Top 10 Districts by Transactions")
                fig3.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                slt.plotly_chart(fig3, use_container_width=True)

            # Top 10 Pincodes by Transactions
            if not df_top_pincodes.empty:
                df_top_pincodes["Total_Transactions"] = pd.to_numeric(df_top_pincodes["Total_Transactions"], errors='coerce').fillna(0)
                fig4 = px.bar(df_top_pincodes, x="Pincode", y="Total_Transactions",
                              color="State", text="Total_Transactions",
                              title="🏆 Top 10 Pincodes by Transactions")
                fig4.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                slt.plotly_chart(fig4, use_container_width=True)

    # ===================================================
    # Scenario 3: Insurance Engagement Analysis 
    # ===================================================
    elif scenario == "Insurance Engagement Analysis":
        available_types = get_transaction_types()
        if not available_types:
            # give them sensible defaults to try, but warn that these are guesses
            available_types = ["Insurance", "Financial Services"]
            slt.info("Could not detect transaction_type column — offering common defaults (you can try these).")

        # choose default index safely
        default_idx = 0
        if "Insurance" in available_types:
            default_idx = available_types.index("Insurance")
        elif "Financial Services" in available_types:
            default_idx = available_types.index("Financial Services")

        tx_type = slt.selectbox("Select insurance-related transaction type", available_types, index=default_idx, key="ins_tx_type")

        # Pull data (robust variants inside functions)
        df_ins_state = insurance_state_summary(year, q_num, tx_type)
        df_ins_district = insurance_top_districts(year, q_num, tx_type)
        df_ins_trend = insurance_quarterly_trend(year, tx_type)
        df_total_state = query_state_transactions_all(year, q_num)  # for share calc (may be empty)

        if df_ins_state.empty:
            slt.warning(f"No data found for '{tx_type}' in {year} {q_label}. Try another transaction type.")
        else:
            # Ensure numeric dtypes
            df_ins_state["Total_Premium"] = pd.to_numeric(df_ins_state["Total_Premium"], errors='coerce').fillna(0)
            if "Policies_Sold" in df_ins_state.columns:
                df_ins_state["Policies_Sold"] = pd.to_numeric(df_ins_state["Policies_Sold"], errors='coerce').fillna(0)

            if not df_total_state.empty and "Total_Transactions" in df_total_state.columns:
                df_total_state["Total_Transactions"] = pd.to_numeric(df_total_state["Total_Transactions"], errors='coerce').fillna(0)
            else:
                # create fallback df_total_state from df_ins_state to avoid merge failing
                df_total_state = pd.DataFrame({"State": df_ins_state["State"].tolist(), "Total_Transactions":[0]*len(df_ins_state)})

            # Merge to compute share safely
            df_share = df_ins_state.merge(df_total_state[["State","Total_Transactions"]], on="State", how="right")
            df_share["Total_Premium"] = pd.to_numeric(df_share.get("Total_Premium", 0), errors='coerce').fillna(0)
            df_share["Total_Transactions"] = pd.to_numeric(df_share.get("Total_Transactions", 0), errors='coerce').fillna(0)
            df_share["Policies_Sold"] = pd.to_numeric(df_share.get("Policies_Sold", 0), errors='coerce').fillna(0)

            # compute share %
            df_share["Insurance_Share_%"] = df_share.apply(
                lambda r: (float(r["Total_Premium"]) / float(r["Total_Transactions"]) * 100) if float(r["Total_Transactions"]) else 0.0, axis=1
            )

            # Choropleth — Insurance Premium across States
            india_geo = load_india_geojson()
            fig1 = px.choropleth(
                df_share, geojson=india_geo,
                featureidkey="properties.ST_NM",
                locations="State", color="Total_Premium",
                hover_data=["Insurance_Share_%","Total_Premium","Total_Transactions"],
                color_continuous_scale="Purples",
                title=f"🗺️ {tx_type} Premium Across States — {year} {q_label}"
            )
            fig1.update_geos(center=dict(lat=22.0, lon=80.0),
                             projection_scale=4.0, visible=False)
            slt.plotly_chart(fig1, use_container_width=True)

            # Top 10 States by Premium with share text
            top_states = df_share.sort_values("Total_Premium", ascending=False).head(10).copy()
            top_states["share_text"] = (pd.to_numeric(top_states["Insurance_Share_%"], errors='coerce').fillna(0).round(2).astype(str) + "%")

            fig2 = px.bar(
                top_states, x="State", y="Total_Premium",
                color="Insurance_Share_%", text="share_text",
                title=f"🏆 Top 10 States by {tx_type} Premium (with Share %)"
            )
            fig2.update_traces(textposition='outside')
            slt.plotly_chart(fig2, use_container_width=True)

            # Top Districts
            if not df_ins_district.empty:
                df_ins_district["Total_Premium"] = pd.to_numeric(df_ins_district["Total_Premium"], errors='coerce').fillna(0)
                if "Policies_Sold" in df_ins_district.columns:
                    df_ins_district["Policies_Sold"] = pd.to_numeric(df_ins_district["Policies_Sold"], errors='coerce').fillna(0)
                fig3 = px.bar(
                    df_ins_district, x="District", y="Total_Premium",
                    color="State", text="Policies_Sold" if "Policies_Sold" in df_ins_district.columns else None,
                    title=f"🏆 Top Districts by {tx_type} Premium"
                )
                fig3.update_traces(textposition='outside')
                slt.plotly_chart(fig3, use_container_width=True)

            # Quarterly Trend (within selected year)
            if not df_ins_trend.empty:
                df_ins_trend["Total_Premium"] = pd.to_numeric(df_ins_trend["Total_Premium"], errors='coerce').fillna(0)
                fig4 = px.line(
                    df_ins_trend.sort_values("Quarter"),
                    x="Quarter", y="Total_Premium",
                    markers=True,
                    title=f"📈 Quarterly {tx_type} Premium Trend — {year}"
                )
                slt.plotly_chart(fig4, use_container_width=True)

            # India-level share donut (display only if values present)
            total_ins = float(df_ins_state["Total_Premium"].sum())
            total_all = float(df_total_state["Total_Transactions"].sum()) if "Total_Transactions" in df_total_state.columns else 0.0
            other_amt = max(total_all - total_ins, 0.0)

            if total_ins == 0 and other_amt == 0:
                slt.info("No monetary data available to show India share donut.")
            else:
                fig5 = go.Figure(go.Pie(
                    labels=[f"{tx_type}", "Others"],
                    values=[total_ins, other_amt],
                    hole=0.5,
                    textinfo="label+percent",
                    hoverinfo="label+value+percent"
                ))
                fig5.update_layout(title=f"🧮 {tx_type} Share vs Others — India, {year} {q_label}")
                slt.plotly_chart(fig5, use_container_width=True)

            # Metrics row
            c1, c2, c3 = slt.columns(3)
            with c1:
                slt.metric("Total Premium (₹)", f"{int(total_ins):,}")
            with c2:
                slt.metric("Total Amount (All Txns, ₹)", f"{int(total_all):,}")
            with c3:
                share_india = (total_ins/total_all*100) if total_all else 0.0
                slt.metric(f"{tx_type} Share (India)", f"{share_india:.2f}%")

            # Raw tables expanders
            with slt.expander("View State-level Table"):
                slt.dataframe(df_share.sort_values("Total_Premium", ascending=False), use_container_width=True)
            if not df_ins_district.empty:
                with slt.expander("View Top Districts Table"):
                    slt.dataframe(df_ins_district, use_container_width=True)

    # ===================================================
    # Scenario 4: Transaction Analysis Across States and Districts
    # ===================================================
    elif scenario == "Transaction Analysis Across States and Districts":
        # fetch data
        df_state = query_state_transactions_all(year, q_num)
        df_top_states = top10_states_transactions(year, q_num)
        df_top_districts = top10_districts_transactions(year, q_num)
        df_top_pincodes = top10_pincodes_transactions(year, q_num)

        if df_state.empty:
            slt.warning("No transaction data available for selection.")
        else:
            india_geo = load_india_geojson()

            # State-level choropleth
            fig_s = px.choropleth(
                df_state, geojson=india_geo,
                featureidkey="properties.ST_NM",
                locations="State", color="Total_Transactions",
                hover_data=["Avg_Transactions","Total_Transactions"],
                color_continuous_scale="Viridis",
                title=f"🗺️ Transaction Volume by State — {year} {q_label}"
            )
            fig_s.update_geos(center=dict(lat=22.0, lon=80.0),
                              projection_scale=4.0, visible=False)
            slt.plotly_chart(fig_s, use_container_width=True)

            # Top 10 States by transaction value
            if not df_top_states.empty:
                df_top_states["Total_Transactions"] = pd.to_numeric(df_top_states["Total_Transactions"], errors='coerce').fillna(0)
                fig_ts = px.bar(
                    df_top_states.sort_values("Total_Transactions", ascending=False),
                    x="State", y="Total_Transactions", text="Total_Transactions",
                    color="Total_Transactions", title=f"🏆 Top 10 States by Transaction Value — {year} {q_label}"
                )
                fig_ts.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_ts.update_layout(xaxis=dict(tickangle=0))
                slt.plotly_chart(fig_ts, use_container_width=True)

            # Top 10 Districts (nationwide)
            if not df_top_districts.empty:
                df_top_districts["Total_Transactions"] = pd.to_numeric(df_top_districts["Total_Transactions"], errors='coerce').fillna(0)
                fig_td = px.bar(
                    df_top_districts.sort_values("Total_Transactions", ascending=False),
                    x="District", y="Total_Transactions", color="State",
                    text="Total_Transactions", title=f"📍 Top 10 Districts by Transaction Value — {year} {q_label}"
                )
                fig_td.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_td.update_layout(xaxis=dict(tickangle=45))
                slt.plotly_chart(fig_td, use_container_width=True)

            # Top 10 Pincodes
            if not df_top_pincodes.empty:
                df_top_pincodes["Total_Transactions"] = pd.to_numeric(df_top_pincodes["Total_Transactions"], errors='coerce').fillna(0)
                fig_tp = px.bar(
                    df_top_pincodes.sort_values("Total_Transactions", ascending=False),
                    x="Pincode", y="Total_Transactions", color="State",
                    text="Total_Transactions", title=f"📮 Top 10 Pincodes by Transaction Value — {year} {q_label}"
                )
                fig_tp.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_tp.update_layout(xaxis=dict(tickangle=45))
                slt.plotly_chart(fig_tp, use_container_width=True)

            # Expanders for raw tables
            with slt.expander("View full State-level transactions table"):
                slt.dataframe(df_state.sort_values("Total_Transactions", ascending=False), use_container_width=True)

            if not df_top_districts.empty:
                with slt.expander("View Top 10 Districts (table)"):
                    slt.dataframe(df_top_districts, use_container_width=True)
            if not df_top_pincodes.empty:
                with slt.expander("View Top 10 Pincodes (table)"):
                    slt.dataframe(df_top_pincodes, use_container_width=True)

    # ===================================================
    # Scenario 5: User Registration Analysis
    # ===================================================
    elif scenario == "User Registration Analysis":
        # fetch registration data
        df_reg_state = query_state_users_all(year, q_num)
        df_top_states_reg = top10_states_users(year, q_num)
        df_top_districts_reg = top10_districts_users(year, q_num)
        df_top_pincodes_reg = top10_pincodes_users(year, q_num)

        if df_reg_state.empty:
            slt.warning("No registration data available for selection.")
        else:
            india_geo = load_india_geojson()

            # Choropleth — Registered Users by State
            # ensure numeric
            df_reg_state["Registered_Users"] = pd.to_numeric(df_reg_state["Registered_Users"], errors='coerce').fillna(0)
            fig_reg_map = px.choropleth(
                df_reg_state, geojson=india_geo,
                featureidkey="properties.ST_NM",
                locations="State", color="Registered_Users",
                hover_data=["Registered_Users"],
                color_continuous_scale="Blues",
                title=f"🗺️ Registered Users by State — {year} {q_label}"
            )
            fig_reg_map.update_geos(center=dict(lat=22.0, lon=80.0),
                                    projection_scale=4.0, visible=False)
            slt.plotly_chart(fig_reg_map, use_container_width=True)

            # Top 10 States by Registrations
            if not df_top_states_reg.empty:
                df_top_states_reg["Registered_Users"] = pd.to_numeric(df_top_states_reg["Registered_Users"], errors='coerce').fillna(0)
                fig_rs = px.bar(
                    df_top_states_reg.sort_values("Registered_Users", ascending=False),
                    x="State", y="Registered_Users", color="Registered_Users",
                    text="Registered_Users", title=f"🏆 Top 10 States by User Registrations — {year} {q_label}"
                )
                fig_rs.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                slt.plotly_chart(fig_rs, use_container_width=True)

            # Top 10 Districts by Registrations (nationwide)
            if not df_top_districts_reg.empty:
                df_top_districts_reg["Registered_Users"] = pd.to_numeric(df_top_districts_reg["Registered_Users"], errors='coerce').fillna(0)
                fig_rd = px.bar(
                    df_top_districts_reg.sort_values("Registered_Users", ascending=False),
                    x="District", y="Registered_Users", color="State",
                    text="Registered_Users", title=f"📍 Top 10 Districts by User Registrations — {year} {q_label}"
                )
                fig_rd.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_rd.update_layout(xaxis=dict(tickangle=45))
                slt.plotly_chart(fig_rd, use_container_width=True)

            # Top 10 Pincodes by Registrations
            if not df_top_pincodes_reg.empty:
                df_top_pincodes_reg["Registered_Users"] = pd.to_numeric(df_top_pincodes_reg["Registered_Users"], errors='coerce').fillna(0)
                fig_rp = px.bar(
                    df_top_pincodes_reg.sort_values("Registered_Users", ascending=False),
                    x="Pincode", y="Registered_Users", color="State",
                    text="Registered_Users", title=f"📮 Top 10 Pincodes by User Registrations — {year} {q_label}"
                )
                fig_rp.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_rp.update_layout(xaxis=dict(tickangle=45))
                slt.plotly_chart(fig_rp, use_container_width=True)

            # Drilldown: select a state to view top districts/pincodes for registrations
            states = df_reg_state["State"].dropna().unique().tolist()
            if states:
                sel_state = slt.selectbox("Select State to drill into registrations", options=states, index=0, key="reg_drill_state")

                # Query top districts for selected state (safe query)
                sql_state_districts = """
                    SELECT district, SUM(registered_users) AS registered_users
                    FROM map_user
                    WHERE year::INT=%s AND quarter::INT=%s AND state=%s
                    GROUP BY district
                    ORDER BY registered_users DESC
                    LIMIT 10;
                """
                df_state_districts = fetch_df(sql_state_districts, (year, q_num, sel_state), ["District","Registered_Users"])

                # Query top pincodes for selected state
                sql_state_pincodes = """
                    SELECT pincode, SUM(registered_users) AS registered_users
                    FROM top_user
                    WHERE year::INT=%s AND quarter::INT=%s AND state=%s
                    GROUP BY pincode
                    ORDER BY registered_users DESC
                    LIMIT 10;
                """
                df_state_pincodes = fetch_df(sql_state_pincodes, (year, q_num, sel_state), ["Pincode","Registered_Users"])

                if not df_state_districts.empty:
                    df_state_districts["Registered_Users"] = pd.to_numeric(df_state_districts["Registered_Users"], errors='coerce').fillna(0)
                    fig_sd = px.bar(df_state_districts, x="District", y="Registered_Users", text="Registered_Users",
                                    title=f"Top Districts for Registrations — {sel_state} ({year} {q_label})")
                    fig_sd.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                    fig_sd.update_layout(xaxis=dict(tickangle=45))
                    slt.plotly_chart(fig_sd, use_container_width=True)
                else:
                    slt.info(f"No district-level registration data available for {sel_state} in {year} {q_label}.")

                if not df_state_pincodes.empty:
                    df_state_pincodes["Registered_Users"] = pd.to_numeric(df_state_pincodes["Registered_Users"], errors='coerce').fillna(0)
                    fig_sp = px.bar(df_state_pincodes, x="Pincode", y="Registered_Users", text="Registered_Users",
                                    title=f"Top Pincodes for Registrations — {sel_state} ({year} {q_label})")
                    fig_sp.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                    fig_sp.update_layout(xaxis=dict(tickangle=45))
                    slt.plotly_chart(fig_sp, use_container_width=True)
                else:
                    slt.info(f"No pincode-level registration data available for {sel_state} in {year} {q_label}.")

            # Summary metrics + share donut
            total_reg = float(df_reg_state["Registered_Users"].sum())
            top5 = df_reg_state.sort_values("Registered_Users", ascending=False).head(5).copy()
            top5_sum = float(top5["Registered_Users"].sum()) if not top5.empty else 0.0
            others = max(total_reg - top5_sum, 0.0)

            c1, c2, c3 = slt.columns(3)
            with c1:
                slt.metric("Total Registered Users", f"{int(total_reg):,}")
            with c2:
                if not top5.empty:
                    top_state_name = top5.iloc[0]["State"]
                    top_state_val = int(top5.iloc[0]["Registered_Users"])
                    slt.metric("Top State (Registrations)", f"{top_state_name} — {top_state_val:,}")
                else:
                    slt.metric("Top State (Registrations)", "N/A")
            with c3:
                slt.metric("Top-5 Share", f"{(top5_sum/total_reg*100):.2f}%" if total_reg else "0%")

            if total_reg > 0:
                fig_p = go.Figure(go.Pie(
                    labels=[*(top5["State"].tolist() if not top5.empty else []), "Others"],
                    values=[*(top5["Registered_Users"].tolist() if not top5.empty else []), others],
                    hole=0.5,
                    textinfo="label+percent",
                    hoverinfo="label+value+percent"
                ))
                fig_p.update_layout(title=f"🔎 Registration Share — Top 5 States vs Others ({year} {q_label})")
                slt.plotly_chart(fig_p, use_container_width=True)

            # Expanders with raw tables
            with slt.expander("View State-level Registration Table"):
                slt.dataframe(df_reg_state.sort_values("Registered_Users", ascending=False), use_container_width=True)
            if not df_top_districts_reg.empty:
                with slt.expander("View Top Districts (nationwide)"):
                    slt.dataframe(df_top_districts_reg.sort_values("Registered_Users", ascending=False), use_container_width=True)
            if not df_top_pincodes_reg.empty:
                with slt.expander("View Top Pincodes (nationwide)"):
                    slt.dataframe(df_top_pincodes_reg.sort_values("Registered_Users", ascending=False), use_container_width=True)

# -----------------------------
# SIDEBAR NAVIGATION
# -----------------------------
slt.sidebar.title("Navigation")
page = slt.sidebar.radio("Go to", ["Home", "Analysis"])

if page == "Home":
    home_page()
elif page == "Analysis":
    analysis_page()
