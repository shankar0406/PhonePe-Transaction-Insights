# Phonepe Pulse Data Visualization and Exploration A User-Friendly Tool Using Streamlit and Plotly

# Introduction:
This project entails cloning the PhonePe Pulse dataset using Git and transforming the data into a DataFrame with Pandas.
The transformed data is then stored in PostgreSQL database utilizing render.
Furthermore, an interactive dashboard is developed with Streamlit and Plotly, incorporating geoplots and various visualization elements to enhance data exploration and insights.

# Domain: FinTech

# Skills Takeaway
Github Cloning
Python Scripting
PostgreSQL Database
render
Streamlit
Plotly

# TECHNOLOGY USED
Python 3.9.
Youtube API
PostgreSQL
Streamlit
Plotly

# Packages and Libraries
import os
import json
import requests
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from typing import List, Dict, Any

# Project Feature
Clone the PhonePe Pulse data from the GitHub repository to your local directory for easy access and streamlined data collection. Effortlessly explore a diverse range of insightful metrics and analytics, providing you with comprehensive information. Make informed decisions with the most current data, ensuring precision in your analyses and strategies.

# Migrate Data to PostgreSQL
Streamline your workflow by converting PhonePe Pulse data from JSON to a DataFrame. Store this organized data in a PostgreSQL database for easy access and efficiency. This process simplifies querying and analysis, providing a strong foundation for data-driven applications and insights.

# Develope Streamlit:
Discover the potential of data exploration with our Streamlit app. It's easy to use, with a user-friendly interface that lets you interact effortlessly with dynamic charts. Customize visualizations and apply filters to tailor your analysis. Zoom in or out smoothly to focus on specific details. Empower your data-driven decisions with this adaptable platform.

# Live Geo Visualization Dashboard:
Enhance your data exploration with a dynamic geo-visualization dashboard made with Streamlit and Plotly. Easily interact with live maps to gain real-time insights from your geographical data. Navigate effortlessly through interactive features to deepen your understanding and make informed decisions using the most up-to-date information available.

# Analysis:
In this Analysis part, it shows seamless performance in Yearly,Quaterly and State wise of the data
