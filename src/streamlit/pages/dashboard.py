import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add project root to sys.path if not present
project_root = Path(__file__).parent.parent.parent.parent.resolve()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.streamlit.utils_ui import (  # noqa: E402
    inject_custom_css,
    apply_plotly_theme,
    render_kpi_card,
    render_insight_box,
    render_page_header,
    create_blood_pressure_matrix,
    create_treemap_chart,
    create_lifestyle_bubble_matrix,
    PRIMARY_COLOR,
    SECONDARY_COLOR,
    ACCENT_CYAN,
    ACCENT_EMERALD,
    ACCENT_AMBER,
    ACCENT_ROSE,
)

# ============================================================================
# PAGE SETUP & CSS
# ============================================================================
st.set_page_config(layout="wide", initial_sidebar_state="expanded")
inject_custom_css()

# ============================================================================
# DATA LOADING
# ============================================================================
@st.cache_data
def load_data():
    """Load the single clean dataset and cache it"""
    path = Path("data/output/clean_fitness_stats.parquet")
    if not path.exists():
        st.warning(
            "⚠️ Processed dataset not found at 'data/output/clean_fitness_stats.parquet'. "
            "Please run the ETL pipeline first."
        )
        st.stop()
    return pd.read_parquet(path)


@st.cache_data
def calculate_global_metrics(df):
    """Pre-calculate baseline global metrics"""
    return {
        "total_participants": df["participant_id"].nunique(),
        "total_activities": len(df),
        "avg_calories": df["calories_burned"].mean(),
        "avg_steps": df["daily_steps"].mean(),
        "avg_fitness": df["fitness_level"].mean(),
        "avg_sleep": df["hours_sleep"].mean(),
        "avg_stress": df["stress_level"].mean(),
        "avg_bmi": df["bmi"].mean(),
    }


df = load_data()
global_metrics = calculate_global_metrics(df)

# Header
render_page_header(
    title="Executive Overview Dashboard",
    subtitle="Comprehensive Health, Activity & Performance Analytics",
    icon="💪",
)

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================
st.sidebar.markdown("## 🔍 Global Filters")
st.sidebar.markdown("Filter dataset across all dashboard analytics")

# Date range filter
min_date = df["date"].min().date() if hasattr(df["date"].min(), "date") else df["date"].min()
max_date = df["date"].max().date() if hasattr(df["date"].max(), "date") else df["date"].max()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Gender filter
gender_options = ["All"] + sorted(list(df["gender"].dropna().unique()))
selected_gender = st.sidebar.selectbox("Gender", gender_options)

# Age range filter
min_age, max_age = int(df["age"].min()), int(df["age"].max())
age_range = st.sidebar.slider(
    "Age Range",
    min_age,
    max_age,
    (min_age, max_age),
)

# Health condition filter
health_options = ["All"] + sorted(list(df["health_condition"].dropna().unique()))
selected_health = st.sidebar.selectbox("Health Condition", health_options)

# Activity type filter
activity_options = ["All"] + sorted(list(df["activity_type"].dropna().unique()))
selected_activity = st.sidebar.multiselect(
    "Activity Type", activity_options, default=["All"]
)

# Filtering execution
filtered_df = df.copy()

if len(date_range) == 2:
    start_d, end_d = date_range[0], date_range[1]
    filtered_df = filtered_df[
        (filtered_df["date"].dt.date >= start_d) & (filtered_df["date"].dt.date <= end_d)
    ]

if selected_gender != "All":
    filtered_df = filtered_df[filtered_df["gender"] == selected_gender]

filtered_df = filtered_df[
    (filtered_df["age"] >= age_range[0]) & (filtered_df["age"] <= age_range[1])
]

if selected_health != "All":
    filtered_df = filtered_df[filtered_df["health_condition"] == selected_health]

if "All" not in selected_activity and selected_activity:
    filtered_df = filtered_df[filtered_df["activity_type"].isin(selected_activity)]

# Sidebar statistics summary badge
pct_active = (len(filtered_df) / len(df) * 100) if len(df) > 0 else 0
st.sidebar.markdown("---")
st.sidebar.info(f"📊 **Active Cohort:** {len(filtered_df):,} / {len(df):,} records ({pct_active:.1f}%)")

# Guard against empty filtered dataset
if filtered_df.empty:
    st.warning("⚠️ No records match the selected sidebar filters. Please broaden your selection.")
    st.stop()

# ============================================================================
# KPI CARDS HERO SECTION
# ============================================================================
st.markdown("### 🚀 Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

# Total Activities
act_count = len(filtered_df)
act_pct = (act_count / global_metrics["total_activities"]) * 100
with col1:
    render_kpi_card(
        title="Activities",
        value=f"{act_count:,}",
        delta=f"{act_pct:.1f}% of total",
        delta_is_positive=True,
        icon="🏃",
        icon_bg="linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
    )

# Unique Participants
user_count = filtered_df["participant_id"].nunique()
user_pct = (user_count / global_metrics["total_participants"]) * 100
with col2:
    render_kpi_card(
        title="Participants",
        value=f"{user_count:,}",
        delta=f"{user_pct:.1f}% active",
        delta_is_positive=True,
        icon="👥",
        icon_bg="linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)",
    )

# Avg Fitness Level
curr_fit = filtered_df["fitness_level"].mean()
diff_fit = ((curr_fit - global_metrics["avg_fitness"]) / global_metrics["avg_fitness"]) * 100
with col3:
    render_kpi_card(
        title="Avg Fitness",
        value=f"{curr_fit:.2f}",
        delta=f"{diff_fit:+.1f}% vs avg",
        delta_is_positive=diff_fit >= 0,
        icon="💪",
        icon_bg="linear-gradient(135deg, #10b981 0%, #059669 100%)",
    )

# Avg Calories Burned
curr_cal = filtered_df["calories_burned"].mean()
diff_cal = ((curr_cal - global_metrics["avg_calories"]) / global_metrics["avg_calories"]) * 100
with col4:
    render_kpi_card(
        title="Avg Calories",
        value=f"{curr_cal:.0f} kcal",
        delta=f"{diff_cal:+.1f}% vs avg",
        delta_is_positive=diff_cal >= 0,
        icon="🔥",
        icon_bg="linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",
    )

# Avg Daily Steps
curr_steps = filtered_df["daily_steps"].mean()
diff_steps = ((curr_steps - global_metrics["avg_steps"]) / global_metrics["avg_steps"]) * 100
with col5:
    render_kpi_card(
        title="Daily Steps",
        value=f"{curr_steps:,.0f}",
        delta=f"{diff_steps:+.1f}% vs avg",
        delta_is_positive=diff_steps >= 0,
        icon="👣",
        icon_bg="linear-gradient(135deg, #06b6d4 0%, #0891b2 100%)",
    )

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# INTERACTIVE ADVANCED VISUALIZATIONS - ROW 1
# ============================================================================
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🩺 Cardiovascular Risk Matrix (Systolic vs Diastolic BP)")
    fig_bp = create_blood_pressure_matrix(filtered_df)
    st.plotly_chart(fig_bp, use_container_width=True)

with col2:
    st.markdown("#### 🗺️ Activity & Intensity Caloric Treemap")
    fig_tree = create_treemap_chart(filtered_df)
    st.plotly_chart(fig_tree, use_container_width=True)

# ============================================================================
# INTERACTIVE ADVANCED VISUALIZATIONS - ROW 2
# ============================================================================
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🫧 Lifestyle Balance: Sleep vs Stress vs Daily Steps")
    fig_bubble = create_lifestyle_bubble_matrix(filtered_df)
    st.plotly_chart(fig_bubble, use_container_width=True)

with col2:
    st.markdown("#### 📦 Fitness Level Distribution by Health Condition")
    fig_box = px.box(
        filtered_df,
        x="health_condition",
        y="fitness_level",
        color="health_condition",
        color_discrete_sequence=[PRIMARY_COLOR, SECONDARY_COLOR, ACCENT_EMERALD, ACCENT_AMBER],
        points="outliers",
    )
    fig_box.update_traces(
        hovertemplate="<b>%{x}</b><br>Fitness Score: %{y:.2f}<extra></extra>"
    )
    fig_box.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Health Condition",
        yaxis_title="Fitness Level (Score)",
    )
    st.plotly_chart(apply_plotly_theme(fig_box), use_container_width=True)

# Executive Insights Callout
top_condition = filtered_df["health_condition"].mode().iloc[0] if not filtered_df["health_condition"].empty else "N/A"
top_act = filtered_df["activity_type"].mode().iloc[0] if not filtered_df["activity_type"].empty else "N/A"

render_insight_box(
    title="Clinical & Fitness Executive Insights",
    content=f"Analysis of <b>{user_count:,} participants</b> logging <b>{act_count:,} sessions</b> shows <b>{top_act}</b> as the dominant activity. "
            f"The Cardiovascular Risk Matrix highlights participant distribution across clinical Blood Pressure ranges, "
            f"while the Caloric Treemap identifies top energy-expenditure workout intensities.",
    icon="💡",
)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# DATA EXPLORER & EXPORT
# ============================================================================
with st.expander("📄 Data Explorer & Export Options", expanded=False):
    st.markdown("##### Filtered Dataset Records")
    st.dataframe(
        filtered_df.head(100),
        use_container_width=True,
        hide_index=True,
    )

    csv_data = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Filtered Data (CSV)",
        data=csv_data,
        file_name="filtered_fitness_dataset.csv",
        mime="text/csv",
    )
