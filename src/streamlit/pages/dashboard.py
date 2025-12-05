import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
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
def load_key_metrics(df=None):
    """Calculate key metrics from the main dataset"""
    if df is None:
        df = load_data()

    metrics = {
        "total_participants": df["participant_id"].nunique(),
        "total_activities": len(df),
        "avg_calories": round(df["calories_burned"].mean(), 1),
        "avg_steps": int(round(df["daily_steps"].mean(), 0)),
        "avg_fitness": round(df["fitness_level"].mean(), 2),
        "avg_sleep": round(df["hours_sleep"].mean(), 1),
        "avg_stress": round(df["stress_level"].mean(), 1),
        "avg_bmi": round(df["bmi"].mean(), 1),
    }

    return pd.DataFrame([metrics])


@st.cache_data
def load_gender_distribution(df=None):
    """Get gender distribution from the main dataset"""
    if df is None:
        df = load_data()

    gender_counts = df["gender"].value_counts().reset_index()
    gender_counts.columns = ["gender", "count"]
    return gender_counts


@st.cache_data
def load_health_condition_distribution(df=None):
    """Get health condition distribution from the main dataset"""
    if df is None:
        df = load_data()

    health_counts = df["health_condition"].value_counts().reset_index()
    health_counts.columns = ["health_condition", "count"]
    return health_counts


@st.cache_data
def load_activity_counts(df=None):
    """Get activity type counts from the main dataset"""
    if df is None:
        df = load_data()

    activity_counts = df["activity_type"].value_counts().reset_index()
    activity_counts.columns = ["activity_type", "count"]
    return activity_counts


@st.cache_data
def load_calories_by_activity(df=None):
    """Get calories burned by activity and intensity"""
    if df is None:
        df = load_data()

    # Just return the relevant columns for the heatmap
    return df[["activity_type", "intensity", "calories_burned"]]


# Load the basic data for filtering options
df = load_data()

# ============================================================================
# HEADER
# ============================================================================
st.title("💪 Fitness Analytics Dashboard")
st.markdown("### Comprehensive Health & Wellness Overview")
st.markdown("---")

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================
st.sidebar.header("🔍 Filters")

# Date range filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(df["date"].min(), df["date"].max()),
    min_value=df["date"].min(),
    max_value=df["date"].max(),
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

# Health condition filter
health_options = ["All"] + list(df["health_condition"].unique())
selected_health = st.sidebar.selectbox("Health Condition", health_options)

# Activity type filter
activity_options = ["All"] + list(df["activity_type"].unique())
selected_activity = st.sidebar.multiselect(
    "Activity Type", activity_options, default=["All"]
)

# Apply filters to basic dataset to show filtered record count
filtered_df = df.copy()

if len(date_range) == 2:
    filtered_df = filtered_df[
        (filtered_df["date"] >= pd.to_datetime(date_range[0]))
        & (filtered_df["date"] <= pd.to_datetime(date_range[1]))
    ]

if selected_gender != "All":
    filtered_df = filtered_df[filtered_df["gender"] == selected_gender]

filtered_df = filtered_df[
    (filtered_df["age"] >= age_range[0]) & (filtered_df["age"] <= age_range[1])
]

if selected_health != "All":
    filtered_df = filtered_df[
        filtered_df["health_condition"] == selected_health
    ]

if "All" not in selected_activity:
    filtered_df = filtered_df[
        filtered_df["activity_type"].isin(selected_activity)
    ]

st.sidebar.markdown(f"**Records:** {len(filtered_df):,} / {len(df):,}")

# ============================================================================
# KPI METRICS
# ============================================================================
st.markdown("## 📈 Key Performance Indicators")

# Load key metrics
key_metrics = load_key_metrics()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_activities = len(filtered_df)
    st.metric(
        label="🏃 Total Activities",
        value=f"{total_activities:,}",
        delta=f"{(total_activities/key_metrics['total_activities'].values[0]*100):.1f}% of total",
    )

with col2:
    unique_users = filtered_df["participant_id"].nunique()
    st.metric(
        label="👥 Unique Users",
        value=f"{unique_users:,}",
        delta=f"{(unique_users/key_metrics['total_participants'].values[0]*100):.1f}% of total",
    )

with col3:
    avg_fitness = filtered_df["fitness_level"].mean()
    overall_avg_fitness = df["fitness_level"].mean()
    st.metric(
        label="💪 Avg Fitness Level",
        value=f"{avg_fitness:.2f}",
        delta=f"{((avg_fitness - overall_avg_fitness)/overall_avg_fitness*100):.1f}%",
    )

with col4:
    avg_calories = filtered_df["calories_burned"].mean()
    st.metric(
        label="🔥 Avg Calories",
        value=f"{avg_calories:.1f}",
        delta=f"{((avg_calories - key_metrics['avg_calories'].values[0])/key_metrics['avg_calories'].values[0]*100):.1f}%",
    )

with col5:
    avg_steps = filtered_df["daily_steps"].mean()
    st.metric(
        label="👣 Avg Daily Steps",
        value=f"{avg_steps:.0f}",
        delta=f"{((avg_steps - key_metrics['avg_steps'].values[0])/key_metrics['avg_steps'].values[0]*100):.1f}%",
    )

st.markdown("---")

# ============================================================================
# GAUGE CHARTS ROW
# ============================================================================
st.markdown("## 🎯 Health Status Gauges")

col1, col2, col3, col4 = st.columns(4)

with col1:
    # Fitness Level Gauge
    fig_fitness = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=filtered_df["fitness_level"].mean(),
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": "Fitness Level", "font": {"size": 20}},
            delta={
                "reference": df["fitness_level"].mean(),
                "increasing": {"color": "green"},
            },
            gauge={
                "axis": {"range": [None, 22]},
                "bar": {"color": "#667eea"},
                "steps": [
                    {"range": [0, 7], "color": "#ffcccc"},
                    {"range": [7, 14], "color": "#ffffcc"},
                    {"range": [14, 22], "color": "#ccffcc"},
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": key_metrics["avg_fitness"].values[0],
                },
            },
        )
    )
    fig_fitness.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig_fitness, use_container_width=True)

with col2:
    # Sleep Hours Gauge
    fig_sleep = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=filtered_df["hours_sleep"].mean(),
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": "Sleep Hours", "font": {"size": 20}},
            delta={
                "reference": df["hours_sleep"].mean(),
                "increasing": {"color": "green"},
            },
            gauge={
                "axis": {"range": [0, 10]},
                "bar": {"color": "#764ba2"},
                "steps": [
                    {"range": [0, 6], "color": "#ffcccc"},
                    {"range": [6, 8], "color": "#ccffcc"},
                    {"range": [8, 10], "color": "#ffffcc"},
                ],
                "threshold": {
                    "line": {"color": "green", "width": 4},
                    "thickness": 0.75,
                    "value": key_metrics["avg_sleep"].values[0],
                },
            },
        )
    )
    fig_sleep.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig_sleep, use_container_width=True)

with col3:
    # Stress Level Gauge (inverted - lower is better)
    fig_stress = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=filtered_df["stress_level"].mean(),
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": "Stress Level", "font": {"size": 20}},
            delta={
                "reference": df["stress_level"].mean(),
                "increasing": {"color": "red"},
            },
            gauge={
                "axis": {"range": [0, 10]},
                "bar": {"color": "#f093fb"},
                "steps": [
                    {"range": [0, 3], "color": "#ccffcc"},
                    {"range": [3, 7], "color": "#ffffcc"},
                    {"range": [7, 10], "color": "#ffcccc"},
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": key_metrics["avg_stress"].values[0],
                },
            },
        )
    )
    fig_stress.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig_stress, use_container_width=True)

with col4:
    # BMI Gauge
    fig_bmi = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=filtered_df["bmi"].mean(),
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": "Average BMI", "font": {"size": 20}},
            delta={
                "reference": df["bmi"].mean(),
                "increasing": {"color": "orange"},
            },
            gauge={
                "axis": {"range": [15, 40]},
                "bar": {"color": "#4facfe"},
                "steps": [
                    {"range": [15, 18.5], "color": "#ffffcc"},
                    {"range": [18.5, 25], "color": "#ccffcc"},
                    {"range": [25, 30], "color": "#ffffcc"},
                    {"range": [30, 40], "color": "#ffcccc"},
                ],
                "threshold": {
                    "line": {"color": "green", "width": 4},
                    "thickness": 0.75,
                    "value": key_metrics["avg_bmi"].values[0],
                },
            },
        )
    )
    fig_bmi.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig_bmi, use_container_width=True)

# ============================================================================
# MAIN VISUALIZATIONS
# ============================================================================
st.markdown("## 📊 Detailed Analytics")

# Row 1: Health Condition & Gender Distribution
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🏥 Health Condition Distribution")
    health_dist = load_health_condition_distribution()

    # Apply filters if needed
    if selected_health != "All":
        health_dist = health_dist[
            health_dist["health_condition"] == selected_health
        ]

    health_dist.columns = ["Health Condition", "Count"]

    fig_health = px.pie(
        health_dist,
        values="Count",
        names="Health Condition",
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4,
    )
    fig_health.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>",
    )
    fig_health.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig_health, use_container_width=True)

with col2:
    st.markdown("### 👥 Gender Distribution")
    gender_dist = load_gender_distribution()

    # Apply filters if needed
    if selected_gender != "All":
        gender_dist = gender_dist[gender_dist["gender"] == selected_gender]

    gender_dist.columns = ["Gender", "Count"]

    fig_gender = px.bar(
        gender_dist,
        x="Gender",
        y="Count",
        color="Gender",
        color_discrete_map={
            "Male": "#4facfe",
            "Female": "#f093fb",
            "Other": "#a8edea",
        },
        text="Count",
    )
    fig_gender.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Count: %{y:,}<extra></extra>",
    )
    fig_gender.update_layout(
        height=400,
        showlegend=False,
        yaxis_title="Number of Activities",
        xaxis_title="",
    )
    st.plotly_chart(fig_gender, use_container_width=True)

# Row 2: Activity Type Distribution & Intensity Level
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🏃 Activity Type Distribution")
    activity_dist = load_activity_counts()

    # Apply filters if needed
    if "All" not in selected_activity:
        activity_dist = activity_dist[
            activity_dist["activity_type"].isin(selected_activity)
        ]

    activity_dist.columns = ["Activity", "Count"]

    fig_activity = px.bar(
        activity_dist.sort_values("Count", ascending=True),
        x="Count",
        y="Activity",
        orientation="h",
        color="Count",
        color_continuous_scale="Viridis",
        text="Count",
    )
    fig_activity.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Count: %{x:,}<extra></extra>",
    )
    fig_activity.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Number of Activities",
        yaxis_title="",
    )
    st.plotly_chart(fig_activity, use_container_width=True)

with col2:
    st.markdown("### ⚡ Intensity Level Distribution")
    intensity_dist = filtered_df["intensity"].value_counts().reset_index()
    intensity_dist.columns = ["Intensity", "Count"]

    color_map = {"Low": "#90EE90", "Medium": "#FFD700", "High": "#FF6347"}

    fig_intensity = px.pie(
        intensity_dist,
        values="Count",
        names="Intensity",
        color="Intensity",
        color_discrete_map=color_map,
        hole=0.4,
    )
    fig_intensity.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>",
    )
    fig_intensity.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig_intensity, use_container_width=True)

# Row 3: Correlation Heatmap & Box Plot
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🔥 Calories by Activity & Intensity")

    # Load pre-filtered data
    calories_by_activity = load_calories_by_activity()

    # Apply additional filters if needed
    if "All" not in selected_activity:
        calories_by_activity = calories_by_activity[
            calories_by_activity["activity_type"].isin(selected_activity)
        ]

    fig_heatmap = px.density_heatmap(
        calories_by_activity,
        x="activity_type",
        y="intensity",
        z="calories_burned",
        histfunc="avg",
        color_continuous_scale="RdYlGn",
        labels={"calories_burned": "Avg Calories"},
    )
    fig_heatmap.update_layout(
        height=400, xaxis_title="Activity Type", yaxis_title="Intensity Level"
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

with col2:
    st.markdown("### 📦 Fitness Level by Health Condition")

    fig_box = px.box(
        filtered_df,
        x="health_condition",
        y="fitness_level",
        color="health_condition",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_box.update_traces(
        hovertemplate="<b>%{x}</b><br>Fitness Level: %{y:.2f}<extra></extra>"
    )
    fig_box.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Health Condition",
        yaxis_title="Fitness Level",
    )
    st.plotly_chart(fig_box, use_container_width=True)

st.markdown("---")
