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

import streamlit as st


def main():
    """
    Main application function that configures and launches the Streamlit dashboard.

    Sets up the page navigation structure and defines the hierarchy of dashboard pages.
    Each page is configured with an appropriate title and icon for improved user experience.
    """
    # Configure the home dashboard page
    dash_board = st.Page("pages/dashboard.py", title="Home", icon="🏠")

    # Configure the progression page
    fitness_progression = st.Page("pages/fitness_dashboard.py", title="Progression", icon="📈")

    # Configure the seasonal patterns analysis page
    seasonal_patterns = st.Page(
        "pages/seasonal_patterns.py", title="Seasonal Trends", icon="📅"
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
