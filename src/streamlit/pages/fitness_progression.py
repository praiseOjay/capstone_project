import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from scipy import stats
from datetime import timedelta

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Fitness Progression Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================================
# CUSTOM CSS
# ============================================================================
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background: white;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    h1 {
        color: #667eea;
        text-align: center;
        padding: 20px 0;
    }
    </style>
""",
    unsafe_allow_html=True,
)


# ============================================================================
# LOAD DATA
# ============================================================================
@st.cache_data
def load_data():
    """Load the single clean dataset and cache it"""
    return pd.read_parquet("data/output/clean_fitness_stats.parquet")


@st.cache_data
def load_participant_data(participant_id, df=None):
    """Get data for a specific participant"""
    if df is None:
        df = load_data()

    return df[df["participant_id"] == participant_id].sort_values("date")


@st.cache_data
def load_progression_summary(df=None):
    """Get the progression summary metrics"""
    if df is None:
        df = load_data()

    # Use the pre-calculated participant metrics
    metrics = [
        "participant_id",
        "fitness_change",
        "fitness_change_pct",
        "workouts_per_week",
        "consistency_score",
        "total_workouts",
        "initial_fitness",
        "current_fitness",
    ]

    # Return only rows where these metrics are populated
    summary_df = (
        df[metrics]
        .dropna(subset=["fitness_change"])
        .drop_duplicates("participant_id")
    )
    return summary_df


@st.cache_data
def load_weekly_data(participant_id, df=None):
    """Calculate weekly stats for a participant on the fly"""
    # Get participant data
    user_df = load_participant_data(participant_id, df)

    if len(user_df) < 7:  # Not enough data
        return None

    # Create weekly aggregations
    weekly_stats = (
        user_df.groupby("week_of_year")
        .agg(
            {
                "fitness_level": "mean",
                "calories_burned": "mean",
                "avg_heart_rate": "mean",
                "resting_heart_rate": "mean",
                "stress_level": "mean",
                "hours_sleep": "mean",
                "bmi": "mean",
                "participant_id": "count",
            }
        )
        .reset_index()
    )

    weekly_stats.columns = [
        "Week",
        "Fitness",
        "Calories",
        "Avg HR",
        "Resting HR",
        "Stress",
        "Sleep",
        "BMI",
        "Workout Count",
    ]

    return weekly_stats


df = load_data()

# ============================================================================
# HEADER
# ============================================================================
st.title("📈 Fitness Progression & Training Analysis")
st.markdown(
    "### Track participant's fitness journey and analyse training impact."
)
st.markdown("---")

# ============================================================================
# SIDEBAR - USER SELECTION & FILTERS
# ============================================================================
st.sidebar.header("🔍 Filters")

# User selection
user_list = sorted(df["participant_id"].unique())
selected_user = st.sidebar.selectbox(
    "Select Participant ID", options=user_list, index=0
)

# Load data for selected user
user_df = load_participant_data(selected_user)

# Date range for selected user
user_date_range = st.sidebar.date_input(
    "Analysis Period",
    value=(user_df["date"].min(), user_df["date"].max()),
    min_value=user_df["date"].min(),
    max_value=user_df["date"].max(),
)

# Apply date filter
if len(user_date_range) == 2:
    user_df = user_df[
        (user_df["date"] >= pd.to_datetime(user_date_range[0]))
        & (user_df["date"] <= pd.to_datetime(user_date_range[1]))
    ]

# Comparison group selection
st.sidebar.markdown("### 📊 Comparison Settings")
compare_by = st.sidebar.selectbox(
    "Compare Against",
    ["All Users", "Same Age Group", "Same Gender", "Same Health Condition"],
)

# Create comparison dataset
if compare_by == "All Users":
    comparison_df = df
elif compare_by == "Same Age Group":
    user_age = user_df["age"].iloc[0]
    age_range = (user_age - 5, user_age + 5)
    comparison_df = df[
        (df["age"] >= age_range[0]) & (df["age"] <= age_range[1])
    ]
elif compare_by == "Same Gender":
    user_gender = user_df["gender"].iloc[0]
    comparison_df = df[df["gender"] == user_gender]
else:  # Same Health Condition
    user_condition = user_df["health_condition"].iloc[0]
    comparison_df = df[df["health_condition"] == user_condition]

st.sidebar.markdown(f"**User Activities:** {len(user_df):,}")
st.sidebar.markdown(
    f"**Comparison Group:** {comparison_df['participant_id'].nunique():,} users"
)

# ============================================================================
# USER PROFILE SUMMARY
# ============================================================================
st.markdown("## 👤 Participant Profile")

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("Age", f"{user_df['age'].iloc[0]} years")

with col2:
    st.metric("Gender", user_df["gender"].iloc[0])

with col3:
    st.metric("BMI", f"{user_df['bmi'].mean():.1f}")

with col4:
    st.metric("Health", user_df["health_condition"].iloc[0])

with col5:
    st.metric("Smoking", user_df["smoking_status"].iloc[0])

with col6:
    st.metric("Activities", f"{len(user_df)}")

st.markdown("---")

# ============================================================================
# SECTION 1: FITNESS LEVEL PROGRESSION
# ============================================================================
st.markdown("## 📊 Fitness Level Progression Over Time")

# Get pre-calculated values if available, otherwise calculate
if "fitness_level_30d_avg" not in user_df.columns:
    user_df["fitness_level_30d_avg"] = (
        user_df["fitness_level"].rolling(window=30, min_periods=1).mean()
    )

if "trend" not in user_df.columns and len(user_df) > 1:
    x_numeric = (user_df["date"] - user_df["date"].min()).dt.days
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        x_numeric, user_df["fitness_level"]
    )
    user_df["trend"] = slope * x_numeric + intercept

# Calculate improvement
initial_fitness = user_df["fitness_level"].iloc[0]
current_fitness = user_df["fitness_level"].iloc[-1]
fitness_change = current_fitness - initial_fitness
fitness_change_pct = (
    (fitness_change / initial_fitness * 100) if initial_fitness != 0 else 0
)

# Create progression chart
fig_progression = go.Figure()

# Actual fitness level
fig_progression.add_trace(
    go.Scatter(
        x=user_df["date"],
        y=user_df["fitness_level"],
        mode="markers",
        name="Actual Fitness",
        marker=dict(size=6, color="#667eea", opacity=0.6),
        hovertemplate="<b>Date:</b> %{x}<br><b>Fitness:</b> %{y:.2f}<extra></extra>",
    )
)

# 30-day moving average
fig_progression.add_trace(
    go.Scatter(
        x=user_df["date"],
        y=user_df["fitness_level_30d_avg"],
        mode="lines",
        name="30-Day Average",
        line=dict(color="#4facfe", width=3),
        hovertemplate="<b>Date:</b> %{x}<br><b>30-Day Avg:</b> %{y:.2f}<extra></extra>",
    )
)

# Trend line
if "trend" in user_df.columns:
    fig_progression.add_trace(
        go.Scatter(
            x=user_df["date"],
            y=user_df["trend"],
            mode="lines",
            name="Trend Line",
            line=dict(color="red", width=2, dash="dash"),
            hovertemplate="<b>Date:</b> %{x}<br><b>Trend:</b> %{y:.2f}<extra></extra>",
        )
    )

# Comparison average
comparison_avg = comparison_df["fitness_level"].mean()
fig_progression.add_hline(
    y=comparison_avg,
    line_dash="dot",
    line_color="green",
    annotation_text=f"Group Average: {comparison_avg:.2f}",
    annotation_position="right",
)

fig_progression.update_layout(
    height=500,
    hovermode="x unified",
    xaxis_title="Date",
    yaxis_title="Fitness Level",
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
    ),
)

st.plotly_chart(fig_progression, use_container_width=True)

# Progression insights
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Initial Fitness",
        f"{initial_fitness:.2f}",
        help="Fitness level at start of period",
    )

with col2:
    st.metric(
        "Current Fitness",
        f"{current_fitness:.2f}",
        delta=f"{fitness_change:+.2f}",
        help="Most recent fitness level",
    )

with col3:
    st.metric(
        "Total Change",
        f"{fitness_change_pct:+.1f}%",
        delta=f"{fitness_change:+.2f} points",
        help="Overall improvement percentage",
    )

with col4:
    percentile = (
        user_df["fitness_level"].mean() > comparison_df["fitness_level"]
    ).mean() * 100
    st.metric(
        "Percentile Rank",
        f"{percentile:.0f}%",
        help="Your ranking vs comparison group",
    )

st.markdown("---")

# ============================================================================
# SECTION 2: CONSISTENT EXERCISE IMPACT
# ============================================================================
st.markdown("## 💪 Impact of Consistent Exercise on Health Metrics")

# Load pre-computed weekly stats if available
weekly_stats = load_weekly_data(selected_user)

# If not available, calculate them
if weekly_stats is None:
    # Calculate exercise frequency
    user_df["days_since_start"] = (
        user_df["date"] - user_df["date"].min()
    ).dt.days
    total_days = user_df["days_since_start"].max() + 1
    exercise_frequency = len(user_df) / (total_days / 7)  # workouts per week

    weekly_stats = (
        user_df.groupby("week_of_year")
        .agg(
            {
                "fitness_level": "mean",
                "calories_burned": "mean",
                "avg_heart_rate": "mean",
                "resting_heart_rate": "mean",
                "stress_level": "mean",
                "hours_sleep": "mean",
                "bmi": "mean",
                "participant_id": "count",
            }
        )
        .reset_index()
    )
    weekly_stats.columns = [
        "Week",
        "Fitness",
        "Calories",
        "Avg HR",
        "Resting HR",
        "Stress",
        "Sleep",
        "BMI",
        "Workout Count",
    ]
else:
    # Calculate exercise frequency from user_df
    user_df["days_since_start"] = (
        user_df["date"] - user_df["date"].min()
    ).dt.days
    total_days = user_df["days_since_start"].max() + 1
    exercise_frequency = len(user_df) / (total_days / 7)  # workouts per week

# Display frequency metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Workout Frequency",
        f"{exercise_frequency:.1f} /week",
        help="Average workouts per week",
    )

with col2:
    avg_duration = user_df["duration_minutes"].mean()
    st.metric(
        "Avg Duration",
        f"{avg_duration:.0f} min",
        help="Average workout duration",
    )

with col3:
    consistency_score = (len(user_df) / total_days) * 100
    st.metric(
        "Consistency Score",
        f"{consistency_score:.1f}%",
        help="Percentage of days with activity",
    )

with col4:
    total_calories = user_df["calories_burned"].sum()
    st.metric(
        "Total Calories",
        f"{total_calories:,.0f}",
        help="Total calories burned in period",
    )

# Multi-metric correlation analysis
st.markdown("### 📈 Health Metrics Evolution")

if len(weekly_stats) > 0:
    # Create 2x2 subplot
    fig_health = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Fitness Level Trend",
            "Resting Heart Rate Trend",
            "Stress Level Trend",
            "Sleep Quality Trend",
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.1,
    )

    # Fitness Level
    fig_health.add_trace(
        go.Scatter(
            x=weekly_stats["Week"],
            y=weekly_stats["Fitness"],
            mode="lines+markers",
            name="Fitness",
            line=dict(color="#667eea", width=3),
            marker=dict(size=8),
        ),
        row=1,
        col=1,
    )

    # Resting Heart Rate
    fig_health.add_trace(
        go.Scatter(
            x=weekly_stats["Week"],
            y=weekly_stats["Resting HR"],
            mode="lines+markers",
            name="Resting HR",
            line=dict(color="#f093fb", width=3),
            marker=dict(size=8),
        ),
        row=1,
        col=2,
    )

    # Stress Level
    fig_health.add_trace(
        go.Scatter(
            x=weekly_stats["Week"],
            y=weekly_stats["Stress"],
            mode="lines+markers",
            name="Stress",
            line=dict(color="#ff6b6b", width=3),
            marker=dict(size=8),
        ),
        row=2,
        col=1,
    )

    # Sleep Hours
    fig_health.add_trace(
        go.Scatter(
            x=weekly_stats["Week"],
            y=weekly_stats["Sleep"],
            mode="lines+markers",
            name="Sleep",
            line=dict(color="#4facfe", width=3),
            marker=dict(size=8),
        ),
        row=2,
        col=2,
    )

    # Update axes
    fig_health.update_xaxes(title_text="Week of Year", row=2, col=1)
    fig_health.update_xaxes(title_text="Week of Year", row=2, col=2)
    fig_health.update_yaxes(title_text="Fitness Level", row=1, col=1)
    fig_health.update_yaxes(title_text="BPM", row=1, col=2)
    fig_health.update_yaxes(title_text="Stress (1-10)", row=2, col=1)
    fig_health.update_yaxes(title_text="Hours", row=2, col=2)

    fig_health.update_layout(
        height=600, showlegend=False, hovermode="x unified"
    )

    st.plotly_chart(fig_health, use_container_width=True)

    # Correlation analysis
    st.markdown("### 🔗 Exercise Frequency vs Health Outcomes")

    col1, col2 = st.columns(2)

    with col1:
        # Workout count vs fitness improvement
        if len(weekly_stats) > 1:
            corr_fitness = (
                weekly_stats[["Workout Count", "Fitness"]].corr().iloc[0, 1]
            )

            fig_corr1 = px.scatter(
                weekly_stats,
                x="Workout Count",
                y="Fitness",
                trendline="ols",
                title=f"Workouts vs Fitness (Correlation: {corr_fitness:.2f})",
                labels={
                    "Workout Count": "Workouts per Week",
                    "Fitness": "Fitness Level",
                },
            )
            fig_corr1.update_traces(marker=dict(size=12, color="#667eea"))
            fig_corr1.update_layout(height=400)
            st.plotly_chart(fig_corr1, use_container_width=True)

    with col2:
        # Workout count vs stress reduction
        if len(weekly_stats) > 1:
            corr_stress = (
                weekly_stats[["Workout Count", "Stress"]].corr().iloc[0, 1]
            )

            fig_corr2 = px.scatter(
                weekly_stats,
                x="Workout Count",
                y="Stress",
                trendline="ols",
                title=f"Workouts vs Stress (Correlation: {corr_stress:.2f})",
                labels={
                    "Workout Count": "Workouts per Week",
                    "Stress": "Stress Level",
                },
            )
            fig_corr2.update_traces(marker=dict(size=12, color="#ff6b6b"))
            fig_corr2.update_layout(height=400)
            st.plotly_chart(fig_corr2, use_container_width=True)
else:
    st.warning(
        "Not enough weekly data available to show health metrics evolution."
    )
st.markdown("---")
