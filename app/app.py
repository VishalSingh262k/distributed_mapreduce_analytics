# Importing required libraries for dashboarding and processing
import streamlit as st
import pandas as pd
import json
import os
import subprocess
import plotly.express as px
import datetime

# Setting page configuration
st.set_page_config(
    page_title="Distributed Wine Data Pipeline Control Center",
    layout="wide"
)

# Defining file paths
MAPREDUCE_OUTPUT = "output/mapreduce_results.json"
DATASET_PATH = "data/generated_big_wine_dataset.csv"

# Defining helper function for logging job events
def logging_job_event(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"[{timestamp}] {message}"

# Defining function for loading MapReduce output
@st.cache_data
def loading_output(file_path):
    if not os.path.exists(file_path):
        return pd.DataFrame()

    parsed = []

    with open(file_path, "r") as f:
        for line in f:
            try:
                wine, json_part = line.strip().split("\t")
                record = json.loads(json_part)
                record["wine_type"] = wine.replace('"', '')
                parsed.append(record)
            except:
                continue

    return pd.DataFrame(parsed)

# Defining function for running MapReduce pipeline
def running_mapreduce_pipeline():
    command = f"python mapreduce/mapreduce_analysis.py {DATASET_PATH} > {MAPREDUCE_OUTPUT}"
    subprocess.run(command, shell=True)

# Loading dataset
df = loading_output(MAPREDUCE_OUTPUT)

# Building dashboard layout
st.title("Distributed Wine Analytics Pipeline Control Center")

# Sidebar Pipeline Controls
st.sidebar.header("Pipeline Controls")

run_pipeline = st.sidebar.button("Run MapReduce Pipeline")

refresh_data = st.sidebar.button("Refresh Dashboard Data")

st.sidebar.markdown("---")
st.sidebar.write("Output File:")
st.sidebar.code(MAPREDUCE_OUTPUT)

# Pipeline Execution Section
if run_pipeline:
    with st.spinner("Running Distributed MapReduce Job..."):
        running_mapreduce_pipeline()
    st.success("Pipeline execution completed successfully")
    st.rerun()

if refresh_data:
    st.cache_data.clear()
    st.rerun()

# Handling empty dataset case
if df.empty:
    st.warning("No pipeline output detected. Please run pipeline first.")
    st.stop()

# Cleaning dataset
df = df[df["feature_index"].notna()]
df["feature_index"] = pd.to_numeric(df["feature_index"], errors="coerce")
df["average_value"] = pd.to_numeric(df["average_value"], errors="coerce")

# KPI METRICS PANEL
st.subheader("Pipeline Metrics Overview")
k1, k2, k3, k4 = st.columns(4)

with k1:
    st.metric("Total Records", len(df))

with k2:
    st.metric("Wine Categories", df["wine_type"].nunique())

with k3:
    st.metric("Avg Concentration", round(df["average_value"].mean(), 3))

with k4:
    st.metric("Max Concentration", round(df["average_value"].max(), 3))


# Data Quality Monitoring
st.subheader("Data Quality Monitoring")

dq1, dq2, dq3 = st.columns(3)

with dq1:
    st.write("Missing Values")
    st.write(df.isnull().sum())

with dq2:
    st.write("Duplicate Rows")
    st.write(df.duplicated().sum())

with dq3:
    st.write("Unique Features")
    st.write(df["feature_index"].nunique())


# Interactive Plotly Visualization
st.subheader("Interactive Feature Distribution")

fig = px.line(
    df,
    x="feature_index",
    y="average_value",
    color="wine_type",
    markers=True,
    title="Feature Concentration Trends"
)

st.plotly_chart(fig, use_container_width=True)

# Top Feature Ranking Explorer
st.subheader("Top Feature Ranking Explorer")
selected_wine = st.selectbox(
    "Selecting Wine Category",
    sorted(df["wine_type"].unique())
)

top_df = (
    df[df["wine_type"] == selected_wine]
    .sort_values("average_value", ascending=False)
    .head(15)
)

st.dataframe(top_df, use_container_width=True)

# Bar Visualization
bar_fig = px.bar(
    top_df,
    x="feature_index",
    y="average_value",
    title=f"Top Features for {selected_wine}"
)

st.plotly_chart(bar_fig, use_container_width=True)

# Raw Distributed Output Log
st.subheader("Raw Distributed Output Log")
with st.expander("View Raw MapReduce Output"):
    if os.path.exists(MAPREDUCE_OUTPUT):
        st.code(open(MAPREDUCE_OUTPUT).read()[:4000])
    else:
        st.warning("Output file not found")
