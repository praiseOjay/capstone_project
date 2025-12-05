import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import calendar

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
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
    .season-card {
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    .winter-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
    .spring-card { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }
    .summer-card { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
    .fall-card { background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    h1 {
        color: #667eea;
        text-align: center;
        padding: 20px 0;
    }
    .insight-box {
        background: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        margin: 10px 0;
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
def load_seasonal_summary(df=None):
    """Create seasonal summary from the main dataset"""
    if df is None:
        df = load_data()

    return (
        df.groupby("season")
        .agg(
            {
                "participant_id": "count",
                "calories_burned": "mean",
                "duration_minutes": "mean",
                "fitness_level": "mean",
                "daily_steps": "mean",
            }
        )
        .round(2)
        .reset_index()
    )


@st.cache_data
def load_monthly_stats(df=None):
    """Create monthly statistics from the main dataset"""
    if df is None:
        df = load_data()

    # Extract month number if needed
    if "month" not in df.columns:
        df = df.assign(month=df["date"].dt.month)

    return (
        df.groupby("month")
        .agg(
            {
                "participant_id": "count",
                "calories_burned": "mean",
                "fitness_level": "mean",
                "daily_steps": "mean",
                "duration_minutes": "mean",
            }
        )
        .reset_index()
    )


@st.cache_data
def load_weekend_comparison(df=None):
    """Create weekend vs weekday comparison"""
    if df is None:
        df = load_data()

    return (
        df.groupby("is_weekend")
        .agg(
            {
                "participant_id": "count",
                "calories_burned": "mean",
                "duration_minutes": "mean",
                "fitness_level": "mean",
                "daily_steps": "mean",
            }
        )
        .round(2)
        .reset_index()
    )


@st.cache_data
def load_intensity_by_season(df=None):
    """Get intensity distribution by season"""
    if df is None:
        df = load_data()

    return df.groupby(["season", "intensity"]).size().reset_index(name="count")


df = load_data()

# ============================================================================
# HEADER
# ============================================================================
st.title("📅 Seasonal Patterns in Exercise Behaviour")
st.markdown("### Comprehensive Analysis of Temporal Exercise Trends")
st.markdown("---")

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================
st.sidebar.header("🔍 Filters")

# Season filter
season_options = ["All"] + sorted(df["season"].unique().tolist())
selected_seasons = st.sidebar.multiselect(
    "Select Seasons", season_options, default=["All"]
)

# Gender filter
gender_options = ["All"] + list(df["gender"].unique())
selected_gender = st.sidebar.selectbox("Gender", gender_options)

# Age range filter
age_range = st.sidebar.slider(
    "Age Range",
    int(df["age"].min()),
    int(df["age"].max()),
    (int(df["age"].min()), int(df["age"].max())),
)

# Activity type filter
activity_options = ["All"] + list(df["activity_type"].unique())
selected_activities = st.sidebar.multiselect(
    "Activity Types", activity_options, default=["All"]
)

# Apply filters
filtered_df = df.copy()

if "All" not in selected_seasons:
    filtered_df = filtered_df[filtered_df["season"].isin(selected_seasons)]

if selected_gender != "All":
    filtered_df = filtered_df[filtered_df["gender"] == selected_gender]

filtered_df = filtered_df[
    (filtered_df["age"] >= age_range[0]) & (filtered_df["age"] <= age_range[1])
]

if "All" not in selected_activities:
    filtered_df = filtered_df[
        filtered_df["activity_type"].isin(selected_activities)
    ]

st.sidebar.markdown(f"**Records:** {len(filtered_df):,} / {len(df):,}")

# ============================================================================
# SEASONAL OVERVIEW CARDS
# ============================================================================
st.markdown("## 🌍 Seasonal Overview")

# Load pre-computed seasonal summary
seasonal_summary = load_seasonal_summary()

col1, col2, col3, col4 = st.columns(4)

seasons_order = ["Winter", "Spring", "Summer", "Fall"]
season_emojis = {"Winter": "❄️", "Spring": "🌸", "Summer": "☀️", "Fall": "🍂"}

for col, season in zip([col1, col2, col3, col4], seasons_order):
    with col:
        if season in seasonal_summary["season"].values:
            data = seasonal_summary[seasonal_summary["season"] == season].iloc[
                0
            ]
            st.markdown(
                f"""
                <div class="season-card {season.lower()}-card">
                    <h3>{season_emojis[season]} {season}</h3>
                    <div class="metric-value">{int(data['participant_id']):,}</div>
                    <div class="metric-label">Activities</div>
                    <hr style="border-color: rgba(255,255,255,0.3); margin: 15px 0;">
                    <div style="font-size: 0.9rem;">
                        🔥 {data['calories_burned']:.1f} cal<br>
                        ⏱️ {data['duration_minutes']:.0f} min<br>
                        💪 {data['fitness_level']:.1f} fitness<br>
                        👣 {data['daily_steps']:.0f} steps
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

st.markdown("---")

# ============================================================================
# SEASONAL COMPARISONS
# ============================================================================
st.markdown("## 📊 Seasonal Performance Comparison")

col1, col2 = st.columns(2)

with col1:
    # Seasonal metrics radar chart
    st.markdown("### 🎯 Multi-Metric Seasonal Comparison")

    # Normalize data for radar chart
    metrics = [
        "calories_burned",
        "duration_minutes",
        "fitness_level",
        "daily_steps",
    ]
    seasonal_normalized = seasonal_summary.copy()

    for metric in metrics:
        max_val = seasonal_normalized[metric].max()
        if max_val > 0:
            seasonal_normalized[metric] = (
                seasonal_normalized[metric] / max_val
            ) * 100

    fig_radar = go.Figure()

    colors = {
        "Winter": "#667eea",
        "Spring": "#f093fb",
        "Summer": "#4facfe",
        "Fall": "#fa709a",
    }

    for season in seasons_order:
        season_data = seasonal_normalized[
            seasonal_normalized["season"] == season
        ]
        if len(season_data) > 0:
            fig_radar.add_trace(
                go.Scatterpolar(
                    r=season_data[metrics].values.tolist()[0],
                    theta=["Calories", "Duration", "Fitness", "Steps"],
                    fill="toself",
                    name=season,
                    line=dict(color=colors[season], width=2),
                    opacity=0.7,
                )
            )

    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=400,
    )

    st.plotly_chart(fig_radar, use_container_width=True)

with col2:
    # Seasonal activity count
    st.markdown("### 📈 Activity Volume by Season")

    season_counts = filtered_df["season"].value_counts().reindex(seasons_order)

    fig_season_bar = go.Figure()

    fig_season_bar.add_trace(
        go.Bar(
            x=season_counts.index,
            y=season_counts.values,
            marker=dict(
                color=[colors[s] for s in season_counts.index],
                line=dict(color="white", width=2),
            ),
            text=season_counts.values,
            texttemplate="%{text:,}",
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Activities: %{y:,}<extra></extra>",
        )
    )

    fig_season_bar.update_layout(
        height=400,
        xaxis_title="Season",
        yaxis_title="Number of Activities",
        showlegend=False,
    )

    st.plotly_chart(fig_season_bar, use_container_width=True)

# ============================================================================
# MONTHLY TRENDS
# ============================================================================
st.markdown("## 📅 Monthly Exercise Patterns")

# Load pre-computed monthly statistics
monthly_stats = load_monthly_stats()
monthly_stats["month_name"] = monthly_stats["month"]

# Create 4 subplots for monthly trends
fig_monthly = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=(
        "Monthly Activity Count",
        "Average Calories Burned",
        "Average Fitness Level",
        "Average Daily Steps",
    ),
    vertical_spacing=0.15,
    horizontal_spacing=0.1,
)

# Activity Count
fig_monthly.add_trace(
    go.Scatter(
        x=monthly_stats["month_name"],
        y=monthly_stats["participant_id"],
        mode="lines+markers",
        name="Activities",
        line=dict(color="#667eea", width=3),
        marker=dict(size=8),
        fill="tozeroy",
        fillcolor="rgba(102, 126, 234, 0.2)",
    ),
    row=1,
    col=1,
)

# Calories
fig_monthly.add_trace(
    go.Scatter(
        x=monthly_stats["month_name"],
        y=monthly_stats["calories_burned"],
        mode="lines+markers",
        name="Calories",
        line=dict(color="#f093fb", width=3),
        marker=dict(size=8),
        fill="tozeroy",
        fillcolor="rgba(240, 147, 251, 0.2)",
    ),
    row=1,
    col=2,
)

# Fitness Level
fig_monthly.add_trace(
    go.Scatter(
        x=monthly_stats["month_name"],
        y=monthly_stats["fitness_level"],
        mode="lines+markers",
        name="Fitness",
        line=dict(color="#4facfe", width=3),
        marker=dict(size=8),
        fill="tozeroy",
        fillcolor="rgba(79, 172, 254, 0.2)",
    ),
    row=2,
    col=1,
)

# Daily Steps
fig_monthly.add_trace(
    go.Scatter(
        x=monthly_stats["month_name"],
        y=monthly_stats["daily_steps"],
        mode="lines+markers",
        name="Steps",
        line=dict(color="#fa709a", width=3),
        marker=dict(size=8),
        fill="tozeroy",
        fillcolor="rgba(250, 112, 154, 0.2)",
    ),
    row=2,
    col=2,
)

fig_monthly.update_layout(height=600, showlegend=False, hovermode="x unified")

st.plotly_chart(fig_monthly, use_container_width=True)

# ============================================================================
# ACTIVITY TYPE BY SEASON
# ============================================================================
st.markdown("## 🏃 Activity Preferences by Season")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📊 Top Activities by Season")

    selected_season_viz = st.selectbox(
        "Select Season to Analyse", seasons_order, key="season_activity"
    )

    season_activities = (
        filtered_df[filtered_df["season"] == selected_season_viz][
            "activity_type"
        ]
        .value_counts()
        .head(10)
        .sort_values(ascending=True)
    )

    fig_top_activities = go.Figure()

    fig_top_activities.add_trace(
        go.Bar(
            x=season_activities.values,
            y=season_activities.index,
            orientation="h",
            marker=dict(
                color=season_activities.values,
                colorscale="Viridis",
                showscale=False,
            ),
            text=season_activities.values,
            texttemplate="%{text:,}",
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Count: %{x:,}<extra></extra>",
        )
    )

    fig_top_activities.update_layout(
        height=500,
        xaxis_title="Number of Activities",
        yaxis_title="",
        showlegend=False,
    )

    st.plotly_chart(fig_top_activities, use_container_width=True)

# ============================================================================
# WEEKEND VS WEEKDAY
# ============================================================================
st.markdown("## 🗓️ Weekend vs Weekday Comparison")

# Load pre-computed weekend comparison data
weekend_comparison = load_weekend_comparison()
weekend_comparison.index = ["Weekday", "Weekend"]

col1, col2, col3, col4 = st.columns(4)

metrics = [
    ("Activities", "participant_id", "🏃"),
    ("Avg Calories", "calories_burned", "🔥"),
    ("Avg Duration (min)", "duration_minutes", "⏱️"),
    ("Avg Fitness", "fitness_level", "💪"),
]

for col, (label, metric, emoji) in zip([col1, col2, col3, col4], metrics):
    with col:
        # Use the string labels instead of numeric indices
        weekday_val = weekend_comparison.loc["Weekday", metric]
        weekend_val = weekend_comparison.loc["Weekend", metric]

        fig_compare = go.Figure()

        fig_compare.add_trace(
            go.Bar(
                x=["Weekday", "Weekend"],
                y=[weekday_val, weekend_val],
                marker=dict(color=["#667eea", "#f093fb"]),
                text=[f"{weekday_val:,.1f}", f"{weekend_val:,.1f}"],
                textposition="outside",
            )
        )

        fig_compare.update_layout(
            title=f"{emoji} {label}",
            height=300,
            showlegend=False,
            yaxis_title="",
            xaxis_title="",
        )

        st.plotly_chart(fig_compare, use_container_width=True)

# ============================================================================
# SEASONAL INTENSITY PATTERNS
# ============================================================================
st.markdown("## ⚡ Intensity Patterns Across Seasons")

# Load pre-computed intensity by season data
intensity_season = load_intensity_by_season()

fig_intensity_season = px.bar(
    intensity_season,
    x="season",
    y="count",
    color="intensity",
    barmode="group",
    color_discrete_map={
        "Low": "#90EE90",
        "Medium": "#FFD700",
        "High": "#FF6347",
    },
    category_orders={"season": seasons_order},
    labels={
        "count": "Number of Activities",
        "season": "Season",
        "intensity": "Intensity Level",
    },
)

fig_intensity_season.update_layout(
    height=400,
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
    ),
)

st.plotly_chart(fig_intensity_season, use_container_width=True)
st.markdown("---")
