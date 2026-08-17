import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import calendar
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
    create_radial_day_chart,
    PRIMARY_COLOR,
    SECONDARY_COLOR,
    ACCENT_CYAN,
    ACCENT_EMERALD,
    ACCENT_AMBER,
    ACCENT_ROSE,
)

# ============================================================================
# PAGE CONFIGURATION & CSS
# ============================================================================
inject_custom_css()

# ============================================================================
# LOAD DATA
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


df = load_data()

# Header
render_page_header(
    title="Seasonal Patterns & Temporal Dynamics",
    subtitle="Evaluating seasonal influences, monthly cycles, and weekly workout rhythms",
    icon="📅",
)

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================
st.sidebar.markdown("## 🔍 Seasonal Filters")

season_options = ["All"] + sorted(list(df["season"].dropna().unique()))
selected_seasons = st.sidebar.multiselect("Select Seasons", season_options, default=["All"])

gender_options = ["All"] + sorted(list(df["gender"].dropna().unique()))
selected_gender = st.sidebar.selectbox("Gender", gender_options)

min_age, max_age = int(df["age"].min()), int(df["age"].max())
age_range = st.sidebar.slider("Age Range", min_age, max_age, (min_age, max_age))

activity_options = ["All"] + sorted(list(df["activity_type"].dropna().unique()))
selected_activities = st.sidebar.multiselect("Activity Types", activity_options, default=["All"])

filtered_df = df.copy()

if "All" not in selected_seasons and selected_seasons:
    filtered_df = filtered_df[filtered_df["season"].isin(selected_seasons)]

if selected_gender != "All":
    filtered_df = filtered_df[filtered_df["gender"] == selected_gender]

filtered_df = filtered_df[
    (filtered_df["age"] >= age_range[0]) & (filtered_df["age"] <= age_range[1])
]

if "All" not in selected_activities and selected_activities:
    filtered_df = filtered_df[filtered_df["activity_type"].isin(selected_activities)]

st.sidebar.markdown("---")
st.sidebar.info(f"📊 **Active Sample:** {len(filtered_df):,} / {len(df):,} sessions")

if filtered_df.empty:
    st.warning("⚠️ No records match the selected sidebar filters.")
    st.stop()

# ============================================================================
# SEASONAL OVERVIEW HERO CARDS
# ============================================================================
st.markdown("### ❄️☀️ Seasonal Performance Summary")

season_stats = (
    filtered_df.groupby("season")
    .agg(
        total_sessions=("participant_id", "count"),
        avg_calories=("calories_burned", "mean"),
        avg_fitness=("fitness_level", "mean"),
        avg_duration=("duration_minutes", "mean"),
    )
    .reset_index()
)

all_seasons = ["Spring", "Summer", "Fall", "Winter"]
season_icons = {"Spring": "🌸", "Summer": "☀️", "Fall": "🍂", "Winter": "❄️"}
season_bgs = {
    "Spring": "linear-gradient(135deg, #f43f5e 0%, #fb7185 100%)",
    "Summer": "linear-gradient(135deg, #06b6d4 0%, #38bdf8 100%)",
    "Fall": "linear-gradient(135deg, #f59e0b 0%, #fbbf24 100%)",
    "Winter": "linear-gradient(135deg, #6366f1 0%, #818cf8 100%)",
}

cols = st.columns(4)

for col, season_name in zip(cols, all_seasons):
    s_row = season_stats[season_stats["season"] == season_name]
    with col:
        if not s_row.empty:
            sess = s_row.iloc[0]["total_sessions"]
            cal = s_row.iloc[0]["avg_calories"]
            fit = s_row.iloc[0]["avg_fitness"]
            dur = s_row.iloc[0]["avg_duration"]
            render_kpi_card(
                title=f"{season_name} Cohort",
                value=f"{sess:,} sessions",
                delta=f"{cal:.0f} kcal | {dur:.0f}m",
                delta_is_positive=True,
                icon=season_icons.get(season_name, "🗓️"),
                icon_bg=season_bgs.get(season_name, PRIMARY_COLOR),
            )
        else:
            render_kpi_card(
                title=f"{season_name} Cohort",
                value="0 sessions",
                delta="No records",
                delta_is_positive=False,
                icon=season_icons.get(season_name, "🗓️"),
            )

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# MONTHLY TREND LINES
# ============================================================================
st.markdown("### 📈 Monthly Trajectory Trends")

if "month" not in filtered_df.columns:
    filtered_df["month"] = filtered_df["date"].dt.month

monthly = (
    filtered_df.groupby("month")
    .agg(
        sessions=("participant_id", "count"),
        avg_calories=("calories_burned", "mean"),
        avg_fitness=("fitness_level", "mean"),
        avg_steps=("daily_steps", "mean"),
    )
    .reset_index()
)

monthly["month_num"] = pd.to_numeric(monthly["month"], errors="coerce").fillna(0).astype(int)
monthly["month_name"] = monthly["month_num"].apply(
    lambda m: calendar.month_name[m] if 1 <= m <= 12 else str(m)
)

col1, col2 = st.columns(2)

with col1:
    st.markdown("##### Monthly Activity Volume")
    fig_vol = px.bar(
        monthly,
        x="month_name",
        y="sessions",
        color="sessions",
        color_continuous_scale="Blues",
        text="sessions",
    )
    fig_vol.update_traces(textposition="outside")
    fig_vol.update_layout(height=340, xaxis_title="", yaxis_title="Total Sessions", coloraxis_showscale=False)
    st.plotly_chart(apply_plotly_theme(fig_vol), use_container_width=True)

with col2:
    st.markdown("##### Monthly Caloric Expenditure & Fitness Level")
    fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
    fig_dual.add_trace(
        go.Scatter(x=monthly["month_name"], y=monthly["avg_calories"], name="Avg Calories (kcal)", line=dict(color=ACCENT_AMBER, width=3)),
        secondary_y=False,
    )
    fig_dual.add_trace(
        go.Scatter(x=monthly["month_name"], y=monthly["avg_fitness"], name="Avg Fitness Score", line=dict(color=PRIMARY_COLOR, width=3, dash="dash")),
        secondary_y=True,
    )
    fig_dual.update_layout(height=340, hovermode="x unified", legend=dict(orientation="h", y=1.1, x=1))
    st.plotly_chart(apply_plotly_theme(fig_dual), use_container_width=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# 7-DAY WORKOUT RHYTHM & WEEKEND DYNAMICS
# ============================================================================
st.markdown("### 🗓️ 7-Day Workout Rhythm & Weekend Dynamics")

col_w_metrics, col_radial = st.columns([1, 1])

w_agg = (
    filtered_df.groupby("is_weekend")
    .agg(
        sessions=("participant_id", "count"),
        calories=("calories_burned", "mean"),
        duration=("duration_minutes", "mean"),
        fitness=("fitness_level", "mean"),
    )
    .round(2)
)

label_map = {False: "Weekday", True: "Weekend", 0: "Weekday", 1: "Weekend", "False": "Weekday", "True": "Weekend"}
w_agg.index = w_agg.index.map(lambda x: label_map.get(x, str(x)))
w_agg = w_agg.reindex(["Weekday", "Weekend"]).fillna(0)

with col_w_metrics:
    st.markdown("##### Weekday vs. Weekend Averages")
    
    wk_sess = w_agg.loc["Weekday", "sessions"]
    wk_cal = w_agg.loc["Weekday", "calories"]
    wk_dur = w_agg.loc["Weekday", "duration"]

    we_sess = w_agg.loc["Weekend", "sessions"]
    we_cal = w_agg.loc["Weekend", "calories"]
    we_dur = w_agg.loc["Weekend", "duration"]

    r1, r2 = st.columns(2)
    with r1:
        render_kpi_card("Weekday Volume", f"{wk_sess:,} sessions", delta=f"{wk_dur:.0f}m avg", icon="💼")
    with r2:
        render_kpi_card("Weekend Volume", f"{we_sess:,} sessions", delta=f"{we_dur:.0f}m avg", icon="🏖️")

    r3, r4 = st.columns(2)
    with r3:
        render_kpi_card("Weekday Burn", f"{wk_cal:.0f} kcal", icon="🔥")
    with r4:
        render_kpi_card("Weekend Burn", f"{we_cal:.0f} kcal", icon="⚡")

with col_radial:
    st.markdown("##### 7-Day Workout Rhythm Clock")
    fig_radial = create_radial_day_chart(filtered_df)
    st.plotly_chart(fig_radial, use_container_width=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# SEASONAL INTENSITY HEATMAP
# ============================================================================
st.markdown("### ⚡ Seasonal Intensity Distribution")

col1, col2 = st.columns(2)

with col1:
    st.markdown("##### Session Count by Season & Intensity")
    intensity_matrix = filtered_df.groupby(["season", "intensity"]).size().unstack(fill_value=0)
    fig_heat = px.imshow(
        intensity_matrix,
        labels=dict(x="Intensity Level", y="Season", color="Session Count"),
        color_continuous_scale="Viridis",
        aspect="auto",
        text_auto=True,
    )
    fig_heat.update_layout(height=340)
    st.plotly_chart(apply_plotly_theme(fig_heat), use_container_width=True)

with col2:
    st.markdown("##### Duration vs Calories across Seasons")
    fig_scat = px.scatter(
        filtered_df,
        x="duration_minutes",
        y="calories_burned",
        color="season",
        color_discrete_sequence=[PRIMARY_COLOR, ACCENT_CYAN, ACCENT_AMBER, ACCENT_ROSE],
        opacity=0.6,
        labels={"duration_minutes": "Duration (min)", "calories_burned": "Calories (kcal)"},
    )
    fig_scat.update_layout(height=340)
    st.plotly_chart(apply_plotly_theme(fig_scat), use_container_width=True)

render_insight_box(
    title="Seasonal Executive Summary",
    content="Analysis indicates consistent workout participation across all seasonal cohorts, "
            "with peak intensity logged during <b>Summer</b> sessions. "
            "Weekday sessions account for the majority of total workout volume.",
    icon="💡",
)
