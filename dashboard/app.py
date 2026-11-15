import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

# Ensure local project modules can be imported when running `streamlit run dashboard/app.py`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from monitoring.checks import run_checks
from quality.advisor import generate_dbt_test_recommendations
from warehouse.store import DEFAULT_DB_PATH, connect

st.set_page_config(
    page_title="shop-dataops | Executive Dashboard & Data Health",
    page_icon="📊",
    layout="wide",
)

DB_PATH = os.getenv("WAREHOUSE_DB_PATH", DEFAULT_DB_PATH)
REJECTED_CSV = Path("data/rejected/rejected_orders.csv")
ALERT_LOG = Path(os.getenv("ALERT_LOG_PATH", "logs/alerts.log"))


def query_df(sql: str) -> pd.DataFrame:
    """Run a read-only SQL query against the DuckDB warehouse."""
    if not Path(DB_PATH).exists():
        return pd.DataFrame()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


st.title("🛒 shop-dataops — Multi-Currency Analytics & Data Health Portal")
st.caption(
    "Automated pipeline from CSV + Frankfurter FX API (`api.frankfurter.dev`) → Contracts → DuckDB → dbt → Streamlit"
)

tab_biz, tab_health, tab_ai = st.tabs(
    [
        "📈 Business Analytics (Q1, Q2, Q3)",
        "🩺 Public Data Health & Quarantine (Innovation #1)",
        "🤖 Contract Evolution & AI Test Advisor (Innovation #2)",
    ]
)

# ---------------------------------------------------------------------------
# TAB 1: BUSINESS ANALYTICS (Q1, Q2, Q3)
# ---------------------------------------------------------------------------
with tab_biz:
    mart_df = query_df("SELECT * FROM mart_revenue_daily ORDER BY order_date, country")
    if mart_df.empty:
        st.warning(
            "Warehouse `mart_revenue_daily` is empty. Run `python orchestration/flow.py --all-valid-days` first."
        )
    else:
        q1_df = query_df(
            """
            SELECT country, ROUND(SUM(revenue_eur), 2) AS total_revenue_eur
            FROM mart_revenue_daily
            GROUP BY country
            ORDER BY total_revenue_eur DESC
            """
        )
        q2_df = query_df(
            """
            SELECT order_date,
                   ROUND(SUM(revenue_eur), 2) AS total_revenue_eur,
                   ROUND(SUM(revenue_usd), 2) AS total_revenue_usd
            FROM mart_revenue_daily
            GROUP BY order_date
            ORDER BY total_revenue_usd DESC NULLS LAST
            """
        )
        q3_df = query_df(
            """
            SELECT country,
                   ROUND(SUM(revenue_eur), 2) AS revenue_eur,
                   ROUND(100.0 * SUM(revenue_eur) / SUM(SUM(revenue_eur)) OVER (), 2) AS share_pct
            FROM mart_revenue_daily
            GROUP BY country
            ORDER BY share_pct DESC
            """
        )

        total_eur = q1_df["total_revenue_eur"].sum()
        total_usd = q2_df["total_revenue_usd"].sum()
        best_day_row = q2_df.iloc[0]

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Total Revenue (EUR)", f"€{total_eur:,.2f}")
        kpi2.metric("Total Revenue (USD)", f"${total_usd:,.2f}")
        kpi3.metric(
            "Q2: Best Day (USD)",
            str(best_day_row["order_date"])[:10],
            f"${best_day_row['total_revenue_usd']:,.2f}",
        )
        kpi4.metric("Active Days Loaded", f"{len(q2_df)} days")

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Q1: Total Revenue per Country (EUR)")
            fig_q1 = px.bar(
                q1_df,
                x="country",
                y="total_revenue_eur",
                color="country",
                text_auto=".2f",
                labels={"total_revenue_eur": "Revenue (EUR)", "country": "Country"},
            )
            st.plotly_chart(fig_q1, use_container_width=True)
            st.dataframe(q1_df, use_container_width=True, hide_index=True)

        with col_right:
            st.subheader("Q3: Share of Total Revenue per Country (%)")
            fig_q3 = px.pie(
                q3_df,
                names="country",
                values="share_pct",
                hole=0.4,
            )
            st.plotly_chart(fig_q3, use_container_width=True)
            st.dataframe(q3_df, use_container_width=True, hide_index=True)

        st.subheader("Q2: Daily Revenue Trend in USD & EUR across All Loaded Days")
        q2_sorted = q2_df.sort_values("order_date")
        fig_q2 = px.line(
            q2_sorted,
            x="order_date",
            y=["total_revenue_usd", "total_revenue_eur"],
            markers=True,
            labels={"value": "Revenue", "order_date": "Order Date", "variable": "Currency"},
        )
        st.plotly_chart(fig_q2, use_container_width=True)
        st.dataframe(q2_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# TAB 2: PUBLIC DATA HEALTH PAGE (INNOVATION #1)
# ---------------------------------------------------------------------------
with tab_health:
    st.subheader("🩺 Live Platform Trust & Data Health Status")
    st.markdown(
        "This public status page lets business users verify whether dashboard figures are **fresh, complete, and contract-verified** before making decisions."
    )

    if Path(DB_PATH).exists():
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            problems = run_checks(con)
            orders_stats = con.execute(
                "SELECT COUNT(*), MIN(order_date), MAX(order_date) FROM raw_orders"
            ).fetchall()[0]
            fx_stats = con.execute(
                "SELECT COUNT(*), MIN(rate_date), MAX(rate_date) FROM raw_fx_rates WHERE currency = 'USD'"
            ).fetchall()[0]
        finally:
            con.close()

        if not problems:
            st.success("✅ ALL HEALTH CHECKS PASSED: Data is fresh, complete, and contract-verified.")
        else:
            for prob in problems:
                st.error(f"🚨 {prob}")

        h1, h2, h3 = st.columns(3)
        h1.metric("Clean Orders in Warehouse", f"{orders_stats[0]} rows", f"{orders_stats[1]} → {orders_stats[2]}")
        h2.metric("EUR→USD FX Days Loaded", f"{fx_stats[0]} days", f"Latest: {fx_stats[2]}")
        rejected_count = len(pd.read_csv(REJECTED_CSV)) if REJECTED_CSV.exists() else 0
        h3.metric("Quarantined Rows (Latest Batch)", f"{rejected_count} rows")

    col_q, col_a = st.columns(2)
    with col_q:
        st.markdown("#### 🛡️ Quarantine Table (`data/rejected/rejected_orders.csv`)")
        if REJECTED_CSV.exists():
            rej_df = pd.read_csv(REJECTED_CSV)
            st.dataframe(rej_df, use_container_width=True, hide_index=True)
        else:
            st.info("No rejected rows file found.")

    with col_a:
        st.markdown("#### 🔔 Recent Alert History (`logs/alerts.log`)")
        if ALERT_LOG.exists():
            lines = ALERT_LOG.read_text(encoding="utf-8").strip().splitlines()
            st.code("\n".join(lines[-15:]) if lines else "No alerts logged yet.", language="text")
        else:
            st.info("No alerts logged yet.")

# ---------------------------------------------------------------------------
# TAB 3: CONTRACT EVOLUTION & AI dbt TEST ADVISOR (INNOVATION #2)
# ---------------------------------------------------------------------------
with tab_ai:
    st.subheader("🤖 Automated dbt Test Advisor & Contract Inspector")
    st.markdown(
        "Inspects active YAML data contracts (`contracts/orders.yaml`, `contracts/frankfurter.yaml`) and warehouse distribution statistics to recommend stricter **dbt schema and singular tests** automatically."
    )
    orders_contract = yaml.safe_load(Path("contracts/orders.yaml").read_text(encoding="utf-8"))
    fx_contract = yaml.safe_load(Path("contracts/frankfurter.yaml").read_text(encoding="utf-8"))

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Active `contracts/orders.yaml`")
        st.json(orders_contract)
    with c2:
        st.markdown("#### Active `contracts/frankfurter.yaml`")
        st.json(fx_contract)

    if Path(DB_PATH).exists():
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            rec = generate_dbt_test_recommendations(con)
        finally:
            con.close()
        st.markdown("#### 💡 Live Statistical Profile & Recommended dbt Tests (`quality/advisor.py`)")
        st.json(rec["profile"])
        st.code(yaml.dump(rec["suggested_dbt_tests"], sort_keys=False), language="yaml")
