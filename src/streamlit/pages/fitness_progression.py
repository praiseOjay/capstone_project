import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
# scipy import replaced with numpy for cross-platform compatibility
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
    create_radar_chart,
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


@st.cache_data
def load_participant_data(participant_id, df):
    """Get data for a specific participant"""
    return df[df["participant_id"] == participant_id].sort_values("date")


df = load_data()

# Header
render_page_header(
    title="Participant Progression & Trajectory Tracker",
    subtitle="In-depth longitudinal analysis of individual fitness evolution and wellness impact",
    icon="📈",
)

# ============================================================================
# SIDEBAR SELECTION & FILTERS
# ============================================================================
st.sidebar.markdown("## 🔍 Participant Selector")

user_list = sorted(list(df["participant_id"].unique()))
selected_user = st.sidebar.selectbox("Select Participant ID", options=user_list, index=0)

user_df = load_participant_data(selected_user, df)

if user_df.empty:
    st.warning("⚠️ No activity history found for the selected participant.")
    st.stop()

# Date range for selected participant
user_min_date = user_df["date"].min().date() if hasattr(user_df["date"].min(), "date") else user_df["date"].min()
user_max_date = user_df["date"].max().date() if hasattr(user_df["date"].max(), "date") else user_df["date"].max()

user_date_range = st.sidebar.date_input(
    "Analysis Period",
    value=(user_min_date, user_max_date),
    min_value=user_min_date,
    max_value=user_max_date,
)

if len(user_date_range) == 2:
    start_d, end_d = user_date_range[0], user_date_range[1]
    user_df = user_df[
        (user_df["date"].dt.date >= start_d) & (user_df["date"].dt.date <= end_d)
    ]

# Cohort comparison settings
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Benchmark Cohort")
compare_by = st.sidebar.selectbox(
    "Benchmark Against",
    ["All Participants", "Same Age Bracket (±5 yrs)", "Same Gender", "Same Health Condition"],
)

if compare_by == "All Participants":
    comparison_df = df
elif compare_by == "Same Age Bracket (±5 yrs)":
    u_age = user_df["age"].iloc[0]
    comparison_df = df[(df["age"] >= u_age - 5) & (df["age"] <= u_age + 5)]
elif compare_by == "Same Gender":
    u_gender = user_df["gender"].iloc[0]
    comparison_df = df[df["gender"] == u_gender]
else:
    u_cond = user_df["health_condition"].iloc[0]
    comparison_df = df[df["health_condition"] == u_cond]

st.sidebar.info(
    f"👤 **Selected User Sessions:** {len(user_df):,}\n\n"
    f"🌐 **Benchmark Cohort:** {comparison_df['participant_id'].nunique():,} participants"
)

# Guard against empty participant dataframe
if user_df.empty:
    st.warning("⚠️ No activities match the selected date range for this participant.")
    st.stop()

# ============================================================================
# PARTICIPANT PROFILE & RADAR COMPARISON
# ============================================================================
st.markdown("### 👤 Participant Profile & Cohort Radar")

col_prof, col_radar = st.columns([1, 1])

with col_prof:
    st.markdown("##### Participant Key Indicators")
    u_age = user_df["age"].iloc[0]
    u_gender = user_df["gender"].iloc[0]
    u_bmi = user_df["bmi"].mean()
    u_cond = user_df["health_condition"].iloc[0]
    u_smoke = user_df.get("smoking_status", pd.Series(["N/A"])).iloc[0]
    u_sessions = len(user_df)

    r1_1, r1_2 = st.columns(2)
    with r1_1:
        render_kpi_card("Age", f"{u_age} yrs", icon="🎂", icon_bg="linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)")
    with r1_2:
        render_kpi_card("Gender", f"{u_gender}", icon="⚧", icon_bg="linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)")

    r2_1, r2_2 = st.columns(2)
    with r2_1:
        render_kpi_card("Avg BMI", f"{u_bmi:.1f}", icon="⚖️", icon_bg="linear-gradient(135deg, #06b6d4 0%, #0891b2 100%)")
    with r2_2:
        render_kpi_card("Health", f"{u_cond}", icon="🏥", icon_bg="linear-gradient(135deg, #10b981 0%, #059669 100%)")

    r3_1, r3_2 = st.columns(2)
    with r3_1:
        render_kpi_card("Smoking", f"{u_smoke}", icon="🫁", icon_bg="linear-gradient(135deg, #f59e0b 0%, #d97706 100%)")
    with r3_2:
        render_kpi_card("Sessions", f"{u_sessions}", icon="🏋️", icon_bg="linear-gradient(135deg, #f43f5e 0%, #e11d48 100%)")

with col_radar:
    st.markdown("##### Multi-Axis Benchmark Spider Radar")
    fig_radar = create_radar_chart(user_df, comparison_df)
    st.plotly_chart(fig_radar, use_container_width=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# FITNESS PROGRESSION & REGRESSION TRENDLINE
# ============================================================================
st.markdown("### 📊 Longitudinal Fitness Score Trajectory")

# Pre-calculate rolling 30d average & linear regression line
user_df = user_df.sort_values("date")
user_df["fitness_30d_avg"] = user_df["fitness_level"].rolling(window=30, min_periods=1).mean()

if len(user_df) > 1:
    x_numeric = (user_df["date"] - user_df["date"].min()).dt.days
    if x_numeric.nunique() > 1:
        slope, intercept = np.polyfit(x_numeric, user_df["fitness_level"], 1)
        user_df["trendline"] = slope * x_numeric + intercept

initial_fitness = user_df["fitness_level"].iloc[0]
current_fitness = user_df["fitness_level"].iloc[-1]
fit_diff = current_fitness - initial_fitness
fit_pct = (fit_diff / initial_fitness * 100) if initial_fitness != 0 else 0

fig_prog = go.Figure()

# Scatter points for actual sessions
fig_prog.add_trace(
    go.Scatter(
        x=user_df["date"],
        y=user_df["fitness_level"],
        mode="markers",
        name="Recorded Session",
        marker=dict(size=7, color=PRIMARY_COLOR, opacity=0.5),
        hovertemplate="<b>Date:</b> %{x|%b %d, %Y}<br><b>Fitness:</b> %{y:.2f}<extra></extra>",
    )
)

# Rolling Average
fig_prog.add_trace(
    go.Scatter(
        x=user_df["date"],
        y=user_df["fitness_30d_avg"],
        mode="lines",
        name="30-Day Moving Average",
        line=dict(color=ACCENT_CYAN, width=3),
        hovertemplate="<b>Date:</b> %{x|%b %d, %Y}<br><b>30d Avg:</b> %{y:.2f}<extra></extra>",
    )
)

# Linear Trend Line
if "trendline" in user_df.columns:
    fig_prog.add_trace(
        go.Scatter(
            x=user_df["date"],
            y=user_df["trendline"],
            mode="lines",
            name="Overall Trendline",
            line=dict(color=ACCENT_ROSE, width=2, dash="dash"),
            hovertemplate="<b>Date:</b> %{x|%b %d, %Y}<br><b>Trend:</b> %{y:.2f}<extra></extra>",
        )
    )

# Benchmark cohort mean line
comp_avg = comparison_df["fitness_level"].mean()
fig_prog.add_hline(
    y=comp_avg,
    line_dash="dot",
    line_color=ACCENT_EMERALD,
    annotation_text=f"Benchmark Mean: {comp_avg:.2f}",
    annotation_position="top right",
)

fig_prog.update_layout(
    height=420,
    hovermode="x unified",
    xaxis_title="Timeline",
    yaxis_title="Fitness Level (Score)",
)

st.plotly_chart(apply_plotly_theme(fig_prog), use_container_width=True)

# Progression Metrics Summary
c1, c2, c3, c4 = st.columns(4)

with c1:
    render_kpi_card("Initial Fitness", f"{initial_fitness:.2f}", icon="🏁")

with c2:
    render_kpi_card(
        "Current Fitness",
        f"{current_fitness:.2f}",
        delta=f"{fit_diff:+.2f} pts",
        delta_is_positive=fit_diff >= 0,
        icon="🎯",
    )

with c3:
    render_kpi_card(
        "Relative Progress",
        f"{fit_pct:+.1f}%",
        delta=f"{fit_diff:+.2f} total delta",
        delta_is_positive=fit_diff >= 0,
        icon="⚡",
    )

with c4:
    rank_pct = (user_df["fitness_level"].mean() > comparison_df["fitness_level"]).mean() * 100
    render_kpi_card("Cohort Percentile", f"{rank_pct:.0f}th", delta="vs benchmark", delta_is_positive=True, icon="🏆")

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# CONSISTENCY & HEALTH OUTCOMES
# ============================================================================
st.markdown("### 💪 Consistent Training & Health Evolution")

# Weekly Aggregation
weekly = (
    user_df.groupby("week_of_year")
    .agg(
        {
            "fitness_level": "mean",
            "calories_burned": "mean",
            "resting_heart_rate": "mean",
            "stress_level": "mean",
            "hours_sleep": "mean",
            "participant_id": "count",
        }
    )
    .reset_index()
    .rename(columns={"participant_id": "workout_count"})
)

total_days = (user_df["date"].max() - user_df["date"].min()).days + 1
workout_freq = (len(user_df) / (total_days / 7)) if total_days > 0 else 0
avg_dur = user_df["duration_minutes"].mean()
consistency_score = min(100.0, (len(user_df) / total_days) * 100) if total_days > 0 else 0
sum_cal = user_df["calories_burned"].sum()

c1, c2, c3, c4 = st.columns(4)

with c1:
    render_kpi_card("Workout Pace", f"{workout_freq:.1f} /wk", icon="📅")

with c2:
    render_kpi_card("Avg Duration", f"{avg_dur:.0f} min", icon="⏱️")

with c3:
    render_kpi_card("Consistency Score", f"{consistency_score:.1f}%", icon="🎯")

with c4:
    render_kpi_card("Cumulative Burn", f"{sum_cal:,.0f} kcal", icon="🔥")

st.markdown("##### Multi-Metric Health Trends")

if not weekly.empty and len(weekly) > 1:
    fig_health = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Fitness Level Trend",
            "Resting Heart Rate (BPM)",
            "Stress Level (1-10)",
            "Sleep Duration (Hours)",
        ),
        vertical_spacing=0.15,
        horizontal_spacing=0.1,
    )

    fig_health.add_trace(
        go.Scatter(x=weekly["week_of_year"], y=weekly["fitness_level"], mode="lines+markers", line=dict(color=PRIMARY_COLOR, width=2.5)),
        row=1, col=1
    )
    fig_health.add_trace(
        go.Scatter(x=weekly["week_of_year"], y=weekly["resting_heart_rate"], mode="lines+markers", line=dict(color=ACCENT_ROSE, width=2.5)),
        row=1, col=2
    )
    fig_health.add_trace(
        go.Scatter(x=weekly["week_of_year"], y=weekly["stress_level"], mode="lines+markers", line=dict(color=ACCENT_AMBER, width=2.5)),
        row=2, col=1
    )
    fig_health.add_trace(
        go.Scatter(x=weekly["week_of_year"], y=weekly["hours_sleep"], mode="lines+markers", line=dict(color=ACCENT_CYAN, width=2.5)),
        row=2, col=2
    )

    fig_health.update_xaxes(title_text="Week of Year", row=2, col=1)
    fig_health.update_xaxes(title_text="Week of Year", row=2, col=2)

    fig_health.update_layout(height=480, showlegend=False, hovermode="x unified")
    st.plotly_chart(apply_plotly_theme(fig_health), use_container_width=True)

    # Executive Trajectory Summary
    render_insight_box(
        title="Progression Trajectory Analysis",
        content=f"Participant <b>{selected_user}</b> maintains an average workout frequency of <b>{workout_freq:.1f} sessions/week</b>. "
                f"Throughout this period, fitness score evolved from <b>{initial_fitness:.2f}</b> to <b>{current_fitness:.2f}</b> "
                f"({fit_pct:+.1f}% change).",
        icon="💡",
    )
else:
    st.info("ℹ️ Insufficient multi-week data available for detailed trend subplots.")
