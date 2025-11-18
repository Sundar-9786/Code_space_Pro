"""
FINAL Streamlit Job Monitoring Web App
with CORRECT JOB OCCURRENCE LOGIC + RADIO FILTER
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import urllib
from datetime import datetime


# ---------------------------
# CONFIG
# ---------------------------

def build_engine():
    conn_str = (
        "postgresql+psycopg2://neondb_owner:npg_Pvn41RHLwuNq@ep-cool-rain-afz8eo0e-pooler.c-2.us-west-2.aws.neon.tech/jobs"
    "?sslmode=require&channel_binding=require&client_encoding=utf8"
    )
    engine = create_engine(conn_str)

    return engine


# ---------------------------
# FETCH DATA
# ---------------------------
def fetch_raw_df(engine):
    now = datetime.now()
    #current_date = now.strftime("%Y%m%d")
    current_date = 20251110

    query = f"""
        SELECT
            j.name AS job_name,
            h.run_date,
            h.step_id,
            h.step_name,
            h.run_time,
            h.run_duration,
            h.message
        FROM jobshistory AS h
        INNER JOIN sysjobs AS j
            ON h.job_id = j.job_id
        WHERE
            h.run_date = %(current_date)s
            AND h.step_id > 0
        ORDER BY
            h.run_date,
            j.name,
            h.run_time,
            h.step_id;
        """

    params = {"current_date": current_date}

    return pd.read_sql(query, engine, params={"current_date": current_date})


# ---------------------------
# TIME NORMALIZATION
# ---------------------------
def convert_time_columns(df):
    df['run_time'] = df['run_time'].astype(str).str.zfill(6)
    df['run_time'] = (
        df['run_time'].str[0:2] + ":" +
        df['run_time'].str[2:4] + ":" +
        df['run_time'].str[4:6]
    )

    df['run_duration'] = df['run_duration'].apply(
        lambda x: f"{x//3600:02d}:{(x%3600)//60:02d}:{x%60:02d}"
    )
    return df


# ---------------------------
# STEP STATUS (STRICT MESSAGE BASED)
# ---------------------------
def derive_step_status(df):
    def detect(msg):
        if not isinstance(msg, str):
            return "Info"
        m = msg.lower().strip()
        if m.endswith("the step failed."):
            return "Error"
        if m.endswith("the step succeeded."):
            return "Success"
        return "Info"
    df["step_status"] = df["message"].apply(detect)
    return df


# ---------------------------
# OCCURRENCE DETECTION (CORRECT LOGIC)
# ---------------------------
def get_job_occurrences(df):
    occurrences = []

    # Step-1 rows mark the start of each occurrence
    step1 = df[df["step_id"] == 1].sort_values(["job_name", "run_date", "run_time"])

    for (job_name, run_date), group in step1.groupby(["job_name", "run_date"]):

        starts = group.sort_values("run_time")["run_time"].tolist()

        for i, start_time in enumerate(starts):
            if i < len(starts) - 1:
                next_start = starts[i+1]
                occ_steps = df[
                    (df["job_name"] == job_name) &
                    (df["run_date"] == run_date) &
                    (df["run_time"] >= start_time) &
                    (df["run_time"] < next_start)
                ]
            else:
                # last occurrence = everything till end
                occ_steps = df[
                    (df["job_name"] == job_name) &
                    (df["run_date"] == run_date) &
                    (df["run_time"] >= start_time)
                ]

            occurrences.append({
                "job_name": job_name,
                "run_date": run_date,
                "start_time": start_time,
                "steps": occ_steps
            })

    return occurrences


# ---------------------------
# LAST OCCURRENCE PER JOB
# ---------------------------
def get_latest_occurrence_per_job(df):
    all_occ = get_job_occurrences(df)
    latest = {}

    for occ in all_occ:
        key = (occ["job_name"], occ["run_date"])
        if key not in latest:
            latest[key] = occ
        else:
            if occ["start_time"] > latest[key]["start_time"]:
                latest[key] = occ

    return list(latest.values())


# ---------------------------
# JOB STATUS DETECTION
# ---------------------------
def determine_job_status(occ):
    msgs = occ["steps"]["message"].astype(str).str.lower().str.strip()

    if msgs.str.endswith("the step failed.").any():
        return "Error"

    if msgs.str.endswith("the step succeeded.").all():
        return "Success"

    return "Info"


# ---------------------------
# BUILD FINAL DF
# ---------------------------
def build_final_df(df):
    occs = get_latest_occurrence_per_job(df)
    final_rows = []

    for occ in occs:
        status = determine_job_status(occ)
        steps = occ["steps"].copy()
        steps["job_status"] = status
        steps["start_time"] = occ["start_time"]
        final_rows.append(steps)

    return pd.concat(final_rows, ignore_index=True)


# ---------------------------
# UI EXPANDER
# ---------------------------
def show_job_expander(job_df, job_name):
    last_run = job_df["start_time"].iloc[0]

    with st.expander(f"{job_name} — Last Run @ {last_run}", False):
        total = len(job_df)
        errors = (job_df["step_status"] == "Error").sum()
        success = (job_df["step_status"] == "Success").sum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Steps", total)
        c2.metric("Success", success)
        c3.metric("Failed", errors)

        st.dataframe(
            job_df[
                ["step_id", "step_name", "run_time", "run_duration", "step_status", "message"]
            ].sort_values("step_id"),
            use_container_width=True
        )


# ---------------------------
# STREAMLIT APP
# ---------------------------
st.set_page_config(page_title="Job Monitoring", layout="wide")
st.title("🧭 SQL Job Monitoring Dashboard")


# Sidebar Filters
st.sidebar.header("Filters")

status_filter = st.sidebar.radio(
    "Job Status Filter",
    ("All", "Success", "Error"),
    index=0
)

search_text = st.sidebar.text_input("Search Job Name", "")

# Refresh Button
if "reload" not in st.session_state:
    st.session_state.reload = False

if st.sidebar.button("Refresh"):
    st.session_state.reload = not st.session_state.reload


# LOAD DATA (only refresh reloads DB)
@st.cache_data(ttl=30)
def load_data(flag):
    engine = build_engine()
    raw = fetch_raw_df(engine)
    raw = convert_time_columns(raw)
    raw = derive_step_status(raw)
    return build_final_df(raw)


with st.spinner("Loading job data..."):
    df_latest = load_data(st.session_state.reload)


# ---------------------------
# SUMMARY (ALL JOBS)
# ---------------------------
job_summary = df_latest.groupby("job_name")["job_status"].first().reset_index()

total_jobs = len(job_summary)
total_success = (job_summary["job_status"] == "Success").sum()
total_error = (job_summary["job_status"] == "Error").sum()

st.markdown("### 📊 Job Summary (Latest Run Only)")
c1, c2, c3 = st.columns(3)
c1.metric("Total Jobs", total_jobs)
c2.metric("Success", total_success)
c3.metric("Failed", total_error)

st.markdown("---")


# ---------------------------
# APPLY FILTERS
# ---------------------------
df_filtered = df_latest.copy()

# Status filter
if status_filter != "All":
    df_filtered = df_filtered[df_filtered["job_status"] == status_filter]

# Search filter
if search_text:
    df_filtered = df_filtered[df_filtered["job_name"].str.contains(search_text, case=False)]



# ---------------------------
# DISPLAY JOBS
# ---------------------------
jobs = sorted(df_filtered["job_name"].unique())

st.subheader(f"Jobs Found: {len(jobs)}")
st.markdown("---")

for job in jobs:
    job_df = df_latest[df_latest["job_name"] == job]
    show_job_expander(job_df, job)
