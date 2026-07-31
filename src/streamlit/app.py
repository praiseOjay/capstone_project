"""
Fitness Analytics Dashboard Application

This module serves as the entry point for the Fitness Analytics dashboard.
It creates a multi-page Streamlit application that visualises fitness statistics
across different dimensions, allowing users to explore patterns and track progression.

The application consists of three main pages:
- Dashboard: Overview of key fitness metrics and summaries
- Fitness Progression: Detailed analysis of fitness improvement over time
- Seasonal Trends: Analysis of how fitness patterns change with seasons

Uses Streamlit's page navigation system to organise content in a user-friendly manner.
"""

import sys
from pathlib import Path
import streamlit as st

# Add project root to sys.path if not present
project_root = Path(__file__).parent.parent.parent.resolve()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.streamlit.utils_ui import inject_custom_css  # noqa: E402


def main():
    """
    Main application function that configures and launches the Streamlit dashboard.
    """
    # Configure initial page settings
    st.set_page_config(
        page_title="Fitness Analytics Dashboard",
        page_icon="💪",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inject shared CSS design system
    inject_custom_css()

    # Configure the home dashboard page
    dash_board = st.Page("pages/dashboard.py", title="Overview Dashboard", icon="🏠")

    # Configure the progression page
    fitness_progression = st.Page("pages/fitness_progression.py", title="Progression Tracker", icon="📈")

    # Configure the seasonal patterns analysis page
    seasonal_patterns = st.Page(
        "pages/seasonal_patterns.py", title="Seasonal & Time Trends", icon="📅"
    )

    # Create the navigation structure with all pages
    pages = st.navigation(
        [
            dash_board,
            fitness_progression,
            seasonal_patterns,
        ]
    )

    # Launch the application with the configured navigation
    pages.run()


if __name__ == "__main__":
    main()
