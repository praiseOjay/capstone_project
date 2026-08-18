"""
Unit tests for src.streamlit.utils_ui module.
Verifies custom UI design helpers, Plotly theme applier, and interactive visual builders.
"""

import pytest
import pandas as pd
import plotly.graph_objects as go
from src.streamlit.utils_ui import (
    apply_plotly_theme,
    create_blood_pressure_matrix,
    create_treemap_chart,
    create_radar_chart,
    create_lifestyle_bubble_matrix,
    create_radial_day_chart,
    render_kpi_card,
    render_insight_box,
    render_page_header,
)


@pytest.fixture
def sample_fitness_df():
    """Provides a sample DataFrame containing all required columns for UI charts."""
    return pd.DataFrame(
        {
            "participant_id": [1, 2, 3, 4, 5],
            "date": pd.date_range("2024-01-01", periods=5),
            "age": [25, 40, 55, 30, 65],
            "gender": ["Male", "Female", "Other", "Female", "Male"],
            "bmi": [21.5, 26.2, 31.0, 19.8, 24.5],
            "health_condition": ["Healthy", "Asthma", "Hypertension", "Healthy", "Diabetes"],
            "activity_type": ["Running", "HIIT", "Cycling", "Yoga", "Walking"],
            "intensity": ["High", "Medium", "Low", "Low", "Medium"],
            "calories_burned": [450, 320, 280, 150, 200],
            "duration_minutes": [45, 30, 60, 40, 50],
            "fitness_level": [12.5, 8.2, 15.0, 10.1, 7.5],
            "daily_steps": [12000, 8500, 6000, 10000, 5000],
            "hours_sleep": [7.5, 6.0, 8.0, 7.0, 6.5],
            "stress_level": [3, 7, 4, 2, 8],
            "hydration_level": [2.5, 1.8, 3.0, 2.0, 1.5],
            "resting_heart_rate": [62, 75, 68, 60, 80],
            "blood_pressure_systolic": [118, 135, 145, 112, 138],
            "blood_pressure_diastolic": [78, 85, 92, 72, 88],
            "day_of_week": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
            "season": ["Winter", "Winter", "Spring", "Spring", "Summer"],
            "is_weekend": [False, False, False, False, False],
        }
    )


def test_apply_plotly_theme():
    """Verify that apply_plotly_theme applies font and transparent background properties."""
    fig = go.Figure(go.Scatter(x=[1, 2], y=[3, 4]))
    themed_fig = apply_plotly_theme(fig)

    assert themed_fig.layout.font.family == "Inter, sans-serif"
    assert themed_fig.layout.paper_bgcolor == "rgba(0,0,0,0)"
    assert themed_fig.layout.plot_bgcolor == "rgba(0,0,0,0)"


def test_create_blood_pressure_matrix(sample_fitness_df):
    """Verify Blood Pressure scatter matrix creation with clinical threshold lines."""
    fig = create_blood_pressure_matrix(sample_fitness_df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) > 0
    # Should have hline and vline annotations
    assert len(fig.layout.shapes) >= 4


def test_create_treemap_chart(sample_fitness_df):
    """Verify Treemap chart generation for activity and intensity."""
    fig = create_treemap_chart(sample_fitness_df)
    assert isinstance(fig, go.Figure)
    assert fig.data[0].type == "treemap"


def test_create_radar_chart(sample_fitness_df):
    """Verify Radar chart creation with participant vs cohort traces."""
    user_df = sample_fitness_df[sample_fitness_df["participant_id"] == 1]
    cohort_df = sample_fitness_df

    fig = create_radar_chart(user_df, cohort_df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 2
    assert fig.data[0].type == "scatterpolar"
    assert fig.data[1].type == "scatterpolar"


def test_create_lifestyle_bubble_matrix(sample_fitness_df):
    """Verify 3D Bubble Plot generation for sleep vs stress vs steps."""
    fig = create_lifestyle_bubble_matrix(sample_fitness_df)
    assert isinstance(fig, go.Figure)
    assert fig.data[0].type == "scatter"


def test_create_radial_day_chart(sample_fitness_df):
    """Verify 7-day Radial Polar Bar chart generation."""
    fig = create_radial_day_chart(sample_fitness_df)
    assert isinstance(fig, go.Figure)
    assert fig.data[0].type == "barpolar"


def test_render_helpers(mocker):
    """Verify render_kpi_card, render_insight_box, and render_page_header invoke st.markdown without errors."""
    mock_markdown = mocker.patch("streamlit.markdown")

    render_kpi_card("Test KPI", "100", "+5%", True)
    mock_markdown.assert_called_once()
    args, kwargs = mock_markdown.call_args
    assert kwargs.get("unsafe_allow_html") is True
    assert "kpi-card" in args[0]
    assert "\n    <div" not in args[0]  # Verify no multiline indentation bug

    mock_markdown.reset_mock()
    render_insight_box("Test Title", "Test Content")
    mock_markdown.assert_called_once()

    mock_markdown.reset_mock()
    render_page_header("Header Title", "Header Subtitle")
    mock_markdown.assert_called_once()


def test_create_blood_pressure_matrix_sampling(sample_fitness_df):
    """Verify Blood Pressure scatter matrix downsamples when rows exceed max_points."""
    large_df = pd.concat([sample_fitness_df] * 20, ignore_index=True)
    fig = create_blood_pressure_matrix(large_df, max_points=10)
    assert isinstance(fig, go.Figure)
    # Total points plotted across all condition traces should equal max_points
    total_pts = sum(len(trace.x) for trace in fig.data if hasattr(trace, "x") and trace.x is not None)
    assert total_pts == 10


def test_create_lifestyle_bubble_matrix_sampling(sample_fitness_df):
    """Verify Lifestyle bubble matrix downsamples when rows exceed max_points."""
    large_df = pd.concat([sample_fitness_df] * 20, ignore_index=True)
    fig = create_lifestyle_bubble_matrix(large_df, max_points=10)
    assert isinstance(fig, go.Figure)
    total_pts = sum(len(trace.x) for trace in fig.data if hasattr(trace, "x") and trace.x is not None)
    assert total_pts == 10

