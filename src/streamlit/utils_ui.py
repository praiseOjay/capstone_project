"""
UI Utility functions, Design System tokens, and Theme Builders for Streamlit.
Provides glassmorphic cards, modern typography, custom KPI metric components,
and consistent Plotly chart styling across all pages.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


# Color Palette Tokens
PRIMARY_COLOR = "#6366f1"     # Indigo
SECONDARY_COLOR = "#8b5cf6"   # Purple / Violet
ACCENT_CYAN = "#06b6d4"       # Cyan
ACCENT_EMERALD = "#10b981"    # Emerald Green
ACCENT_AMBER = "#f59e0b"      # Amber / Orange
ACCENT_ROSE = "#f43f5e"       # Rose / Pink
TEXT_DARK = "#1e293b"         # Slate 800
TEXT_MUTED = "#64748b"        # Slate 500
BG_CARD = "#ffffff"


def inject_custom_css():
    """Injects custom CSS design system into the Streamlit app"""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700&display=swap');

        html, body, [class*="css"], div[data-testid="stAppViewContainer"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        h1, h2, h3, h4, h5, h6 {
            font-family: 'Outfit', sans-serif !important;
            font-weight: 600 !important;
            letter-spacing: -0.02em;
        }

        /* Glassmorphic Metric Cards */
        .kpi-card {
            background: rgba(30, 41, 59, 0.7);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 14px;
            padding: 14px 14px;
            box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.25);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
            margin-bottom: 12px;
            min-height: 105px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }

        .kpi-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 12px 28px -4px rgba(99, 102, 241, 0.3);
            border-color: rgba(99, 102, 241, 0.4);
        }

        .kpi-card-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 6px;
            gap: 6px;
        }

        .kpi-card-title {
            font-size: 0.76rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #94a3b8;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .kpi-icon-badge {
            width: 32px;
            height: 32px;
            min-width: 32px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.05rem;
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.2);
        }

        .kpi-value {
            font-family: 'Outfit', sans-serif;
            font-size: 1.4rem;
            font-weight: 700;
            color: #ffffff;
            line-height: 1.25;
            margin-bottom: 4px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            word-break: normal;
        }

        .kpi-delta-tag {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            font-size: 0.78rem;
            font-weight: 600;
            padding: 3px 9px;
            border-radius: 20px;
        }

        .delta-positive {
            background-color: rgba(16, 185, 129, 0.18);
            color: #34d399;
            border: 1px solid rgba(52, 211, 153, 0.3);
        }

        .delta-negative {
            background-color: rgba(239, 68, 68, 0.18);
            color: #f87171;
            border: 1px solid rgba(248, 113, 113, 0.3);
        }

        .delta-neutral {
            background-color: rgba(148, 163, 184, 0.18);
            color: #cbd5e1;
            border: 1px solid rgba(203, 213, 225, 0.3);
        }

        /* Insight Box */
        .insight-card {
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%);
            border-left: 4px solid #6366f1;
            border-radius: 12px;
            padding: 16px 20px;
            margin: 16px 0;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.2);
            border-top: 1px solid rgba(255, 255, 255, 0.05);
            border-right: 1px solid rgba(255, 255, 255, 0.05);
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        .insight-title {
            font-weight: 700;
            color: #818cf8;
            font-size: 0.95rem;
            margin-bottom: 6px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .insight-body {
            font-size: 0.88rem;
            color: #cbd5e1;
            line-height: 1.5;
        }

        /* Premium Dark Sidebar Styling */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f172a 0%, #1e1b4b 100%) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
        }

        section[data-testid="stSidebar"] *, 
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: #f1f5f9 !important;
        }

        /* Sidebar Inputs, Selectboxes, and Widgets styling */
        section[data-testid="stSidebar"] div[data-baseweb="select"] > div,
        section[data-testid="stSidebar"] div[data-baseweb="input"] > div,
        section[data-testid="stSidebar"] input {
            background-color: rgba(30, 41, 59, 0.8) !important;
            border: 1px solid rgba(255, 255, 255, 0.15) !important;
            color: #ffffff !important;
            border-radius: 8px !important;
        }

        section[data-testid="stSidebar"] div[data-baseweb="select"] span {
            color: #ffffff !important;
        }

        /* Sidebar navigation items / links */
        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a {
            background-color: transparent !important;
            color: #cbd5e1 !important;
            border-radius: 8px !important;
            margin-bottom: 4px !important;
            transition: all 0.2s ease !important;
        }

        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {
            background-color: rgba(99, 102, 241, 0.2) !important;
            color: #ffffff !important;
        }

        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%) !important;
            color: #ffffff !important;
            font-weight: 600 !important;
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3) !important;
        }

        /* Sidebar info box callout */
        section[data-testid="stSidebar"] div[data-testid="stAlert"] {
            background-color: rgba(30, 41, 59, 0.7) !important;
            border: 1px solid rgba(99, 102, 241, 0.3) !important;
            color: #e2e8f0 !important;
        }

        /* Styled Dividers */
        .section-divider {
            height: 1px;
            background: linear-gradient(90deg, rgba(99, 102, 241, 0.3) 0%, rgba(139, 92, 246, 0.3) 50%, rgba(226, 232, 240, 0) 100%);
            margin: 24px 0 28px 0;
            border: none;
        }

        /* Header Accent Banner */
        .page-header {
            padding: 10px 0 20px 0;
        }

        .page-subtitle {
            color: #94a3b8;
            font-size: 1.05rem;
            font-weight: 400;
            margin-top: -8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_plotly_theme(fig):
    """Applies a consistent modern theme to Plotly figures"""
    fig.update_layout(
        font=dict(family="Inter, sans-serif", size=12, color="#cbd5e1"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=30),
        hoverlabel=dict(
            bgcolor="#1e293b",
            font_size=12,
            font_family="Inter, sans-serif",
            font_color="#ffffff",
        ),
        xaxis=dict(
            gridcolor="rgba(241, 245, 249, 0.1)",
            zerolinecolor="rgba(226, 232, 240, 0.15)",
            title_font=dict(size=12, color="#94a3b8", family="Outfit, sans-serif"),
            tickfont=dict(size=11, color="#94a3b8"),
        ),
        yaxis=dict(
            gridcolor="rgba(241, 245, 249, 0.1)",
            zerolinecolor="rgba(226, 232, 240, 0.15)",
            title_font=dict(size=12, color="#94a3b8", family="Outfit, sans-serif"),
            tickfont=dict(size=11, color="#94a3b8"),
        ),
        legend=dict(
            font=dict(size=11, color="#cbd5e1"),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )
    return fig


def render_kpi_card(
    title: str,
    value: str,
    delta: str = None,
    delta_is_positive: bool = True,
    icon: str = "📊",
    icon_bg: str = "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
    icon_color: str = "#ffffff",
):
    """Renders a modern glassmorphic KPI card with badge icon and delta tag"""
    delta_html = ""
    if delta:
        delta_class = (
            "delta-positive"
            if delta_is_positive
            else ("delta-negative" if delta_is_positive is False else "delta-neutral")
        )
        arrow = "↑" if delta_is_positive else ("↓" if delta_is_positive is False else "•")
        delta_html = (
            f'<div class="kpi-delta-tag {delta_class}">'
            f'<span>{arrow}</span><span>{delta}</span>'
            f'</div>'
        )

    card_html = (
        f'<div class="kpi-card">'
        f'<div class="kpi-card-header">'
        f'<span class="kpi-card-title">{title}</span>'
        f'<div class="kpi-icon-badge" style="background: {icon_bg}; color: {icon_color};">{icon}</div>'
        f'</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{delta_html}'
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)


def render_insight_box(title: str, content: str, icon: str = "💡"):
    """Renders an executive insight callout box"""
    html = (
        f'<div class="insight-card">'
        f'<div class="insight-title"><span>{icon}</span><span>{title}</span></div>'
        f'<div class="insight-body">{content}</div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def render_page_header(title: str, subtitle: str, icon: str = "💪"):
    """Renders a clean, unified page header banner"""
    html = (
        f'<div class="page-header">'
        f'<h1 style="color: #818cf8; font-size: 2.2rem; display: flex; align-items: center; gap: 12px; margin-bottom: 6px;">'
        f'<span>{icon}</span><span>{title}</span>'
        f'</h1>'
        f'<p class="page-subtitle">{subtitle}</p>'
        f'</div>'
        f'<div class="section-divider"></div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def create_blood_pressure_matrix(df):
    """
    Creates a clinical-grade Blood Pressure quadrant scatter plot
    comparing Systolic vs Diastolic BP against medical risk thresholds.
    """
    bp_df = df.dropna(subset=["blood_pressure_systolic", "blood_pressure_diastolic"]).copy()
    if bp_df.empty:
        return go.Figure()

    fig = px.scatter(
        bp_df,
        x="blood_pressure_diastolic",
        y="blood_pressure_systolic",
        color="health_condition",
        size="age",
        hover_data=["participant_id", "gender", "bmi", "stress_level"],
        color_discrete_sequence=[PRIMARY_COLOR, ACCENT_CYAN, SECONDARY_COLOR, ACCENT_EMERALD, ACCENT_AMBER],
        labels={
            "blood_pressure_diastolic": "Diastolic BP (mmHg)",
            "blood_pressure_systolic": "Systolic BP (mmHg)",
            "health_condition": "Health Condition",
        },
        opacity=0.75,
    )

    # Add clinical reference lines for hypertension thresholds
    fig.add_hline(y=120, line_dash="dash", line_color="rgba(16, 185, 129, 0.6)", annotation_text="Elevated Systolic (120)", annotation_position="bottom right")
    fig.add_hline(y=140, line_dash="dash", line_color="rgba(239, 68, 68, 0.6)", annotation_text="Stage 2 High BP (140)", annotation_position="top right")
    fig.add_vline(x=80, line_dash="dash", line_color="rgba(16, 185, 129, 0.6)", annotation_text="Elevated Diastolic (80)", annotation_position="top left")
    fig.add_vline(x=90, line_dash="dash", line_color="rgba(239, 68, 68, 0.6)", annotation_text="Stage 2 High BP (90)", annotation_position="bottom right")

    fig.update_layout(height=400)
    return apply_plotly_theme(fig)


def create_treemap_chart(df):
    """
    Creates an interactive Treemap chart breaking down Activity Type ➔ Intensity Level,
    sized by session count and colored by average calories burned.
    """
    tree_df = df.groupby(["activity_type", "intensity"]).agg(
        session_count=("participant_id", "count"),
        avg_calories=("calories_burned", "mean"),
    ).reset_index()

    fig = px.treemap(
        tree_df,
        path=["activity_type", "intensity"],
        values="session_count",
        color="avg_calories",
        color_continuous_scale="Purples",
        hover_data={"avg_calories": ":.1f kcal", "session_count": True},
        labels={"avg_calories": "Avg Calories", "session_count": "Sessions"},
    )
    fig.update_traces(
        marker=dict(cornerradius=6),
        hovertemplate="<b>%{label}</b><br>Sessions: %{value:,}<br>Avg Burn: %{color:.1f} kcal<extra></extra>",
    )
    fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))
    return apply_plotly_theme(fig)


def create_radar_chart(user_df, cohort_df):
    """
    Creates a 6-axis Radar / Spider chart comparing normalized participant scores
    directly against their benchmark cohort mean.
    """
    categories = ["Fitness Level", "Daily Steps", "Sleep Duration", "Hydration", "Heart Rate Control", "Healthy BMI"]

    def calc_scores(target_df):
        fit_score = min(100.0, (target_df["fitness_level"].mean() / 20.0) * 100.0)
        steps_score = min(100.0, (target_df["daily_steps"].mean() / 20000.0) * 100.0)
        sleep_score = min(100.0, (target_df["hours_sleep"].mean() / 9.0) * 100.0)
        hydr_score = min(100.0, (target_df["hydration_level"].mean() / 10.0) * 100.0)
        
        # Inverted resting heart rate (lower is better: 50 bpm = 100, 100 bpm = 0)
        r_hr = target_df["resting_heart_rate"].mean()
        hr_score = max(0.0, min(100.0, (100.0 - r_hr) * 2.0))
        
        # BMI Proximity to 22.0
        bmi_val = target_df["bmi"].mean()
        bmi_score = max(0.0, min(100.0, (1.0 - abs(bmi_val - 22.0) / 15.0) * 100.0))

        return [fit_score, steps_score, sleep_score, hydr_score, hr_score, bmi_score]

    user_scores = calc_scores(user_df)
    cohort_scores = calc_scores(cohort_df)

    fig = go.Figure()

    fig.add_trace(
        go.Scatterpolar(
            r=user_scores + [user_scores[0]],
            theta=categories + [categories[0]],
            fill="toself",
            name="Selected Participant",
            fillcolor="rgba(99, 102, 241, 0.35)",
            line=dict(color=PRIMARY_COLOR, width=3),
        )
    )

    fig.add_trace(
        go.Scatterpolar(
            r=cohort_scores + [cohort_scores[0]],
            theta=categories + [categories[0]],
            fill="toself",
            name="Cohort Benchmark Mean",
            fillcolor="rgba(16, 185, 129, 0.2)",
            line=dict(color=ACCENT_EMERALD, width=2, dash="dash"),
        )
    )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], gridcolor="rgba(255,255,255,0.15)", tickfont=dict(size=10, color="#94a3b8")),
            angularaxis=dict(gridcolor="rgba(255,255,255,0.15)", tickfont=dict(size=11, color="#cbd5e1")),
            bgcolor="rgba(0,0,0,0)",
        ),
        height=380,
        showlegend=True,
    )
    return apply_plotly_theme(fig)


def create_lifestyle_bubble_matrix(df):
    """
    Creates an interactive 3D Bubble Plot of Sleep Hours vs Stress Level vs Daily Steps vs Fitness Score.
    """
    bubble_df = df.dropna(subset=["hours_sleep", "stress_level", "daily_steps", "fitness_level"]).copy()
    if bubble_df.empty:
        return go.Figure()

    fig = px.scatter(
        bubble_df,
        x="hours_sleep",
        y="stress_level",
        size="daily_steps",
        color="fitness_level",
        color_continuous_scale="Tealgrn",
        hover_data=["participant_id", "activity_type", "bmi"],
        labels={
            "hours_sleep": "Sleep Duration (Hours)",
            "stress_level": "Stress Index (1-10)",
            "daily_steps": "Daily Steps",
            "fitness_level": "Fitness Score",
        },
        opacity=0.8,
    )

    fig.update_layout(height=400)
    return apply_plotly_theme(fig)


def create_radial_day_chart(df):
    """
    Creates a 7-day Radial Polar Bar chart displaying workout volume and intensity by Day of the Week.
    """
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    day_stats = df.groupby("day_of_week").agg(
        sessions=("participant_id", "count"),
        avg_calories=("calories_burned", "mean"),
    ).reindex(day_order).fillna(0).reset_index()

    fig = go.Figure()

    fig.add_trace(
        go.Barpolar(
            r=day_stats["sessions"],
            theta=day_stats["day_of_week"],
            marker=dict(
                color=day_stats["avg_calories"],
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title="Avg kcal"),
            ),
            hovertemplate="<b>%{theta}</b><br>Sessions: %{r:,}<br>Avg Burn: %{marker.color:.1f} kcal<extra></extra>",
        )
    )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, gridcolor="rgba(255,255,255,0.15)", tickfont=dict(color="#94a3b8")),
            angularaxis=dict(gridcolor="rgba(255,255,255,0.15)", tickfont=dict(color="#cbd5e1")),
            bgcolor="rgba(0,0,0,0)",
        ),
        height=380,
        showlegend=False,
    )
    return apply_plotly_theme(fig)
