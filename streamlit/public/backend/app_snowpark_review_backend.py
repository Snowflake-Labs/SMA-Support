from audioop import avg

import streamlit as st
import math
import public.backend.app_snowpark_utils as utils
from public.backend.query_quality_handler import with_table_quality_handler
from public.backend.globals import *
from snowflake.snowpark.functions import col, max as max_, round, avg, \
    to_char, regexp_replace


@st.cache_data(show_spinner=True)
def get_date_last_execution(execution_ids):
    session = utils.get_session()
    df = with_table_quality_handler(session.table(VIEW_CACHED_DATA_EXECUTION_SUMMARY), VIEW_CACHED_DATA_EXECUTION_SUMMARY)

    if execution_ids is None or len(execution_ids) <= 0:
        result = df.select(to_char(max_(col(COLUMN_TOOL_EXECUTION_TIMESTAMP)), 'yyyy-MM-dd').alias('MAX_DATE')).to_pandas()
    else:
        result = df.where(col(COLUMN_TOOL_EXECUTION_ID).isin(execution_ids)) \
            .select(to_char(max_(col(COLUMN_TOOL_EXECUTION_TIMESTAMP)), 'yyyy-MM-dd').alias('MAX_DATE')) \
            .to_pandas()

    if result.empty or result.MAX_DATE.isnull().all():
        return 0
    else:
        return result.MAX_DATE.iloc[0]

@st.cache_data(show_spinner=True)
def get_sql_readiness_score(executionIds):
    session = utils.get_session()
    if executionIds is None or len(executionIds) <= 0:
        result = (
            session.table(VIEW_CACHED_DATA_EXECUTION_SUMMARY)
            .where(
                (col("SQL_READINESS_SCORE") != '') &
                (col("SQL_READINESS_SCORE") != 'N/A**') &
                (col("SQL_READINESS_SCORE") != 'N/A') &
                (col("SQL_READINESS_SCORE").isNotNull())
            )
            .withColumn("SQL_READINESS_SCORE_FLOAT", regexp_replace(regexp_replace(col("SQL_READINESS_SCORE"), '%', ''), ',', '.').cast("float"))
            .agg(
                round(avg(col("SQL_READINESS_SCORE_FLOAT")), 2)
                .alias("AVERAGE_READINESS")
            )
            .to_pandas()
            .AVERAGE_READINESS
        )
    else:
        result = (
            session.table(VIEW_CACHED_DATA_EXECUTION_SUMMARY)
            .where(
                (col(COLUMN_TOOL_EXECUTION_ID).isin(executionIds)) &
                (col("SQL_READINESS_SCORE") != '') &
                (col("SQL_READINESS_SCORE") != 'N/A**') &
                (col("SQL_READINESS_SCORE") != 'N/A') &
                (col("SQL_READINESS_SCORE").isNotNull())
            )
            .withColumn("SQL_READINESS_SCORE_FLOAT", regexp_replace(regexp_replace(col("SQL_READINESS_SCORE"), '%', ''), ',', '.').cast("float"))
            .agg(
                round(avg(col("SQL_READINESS_SCORE_FLOAT")), 2)
                .alias("AVERAGE_READINESS")
            )
            .to_pandas()
            .AVERAGE_READINESS
        )
    if result is None or result[0] is None or math.isnan(result[0]) :
        return 0
    else:
        return float(result)

@st.cache_data(show_spinner=True)
def get_third_party_readiness_score(executionIds):
    session = utils.get_session()
    if executionIds is None or len(executionIds) <= 0:
        result = (
            session.table(VIEW_CACHED_DATA_EXECUTION_SUMMARY)
            .where(
                (col("THIRD_PARTY_READINESS_SCORE") != 'N/A') &
                (col("THIRD_PARTY_READINESS_SCORE").isNotNull())
            )
            .withColumn("THIRD_PARTY_READINESS_SCORE_FLOAT", col("THIRD_PARTY_READINESS_SCORE").cast("float"))
            .agg(
                round(avg(col("THIRD_PARTY_READINESS_SCORE_FLOAT")), 2)
                .alias("AVERAGE_READINESS")
            )
            .to_pandas()
            .AVERAGE_READINESS
        )
    else:
        result = (
            session.table(VIEW_CACHED_DATA_EXECUTION_SUMMARY)
            .where(
                (col(COLUMN_TOOL_EXECUTION_ID).isin(executionIds)) &
                (col("THIRD_PARTY_READINESS_SCORE") != 'N/A') &
                (col("THIRD_PARTY_READINESS_SCORE").isNotNull())
            )
            .withColumn("THIRD_PARTY_READINESS_SCORE_FLOAT", col("THIRD_PARTY_READINESS_SCORE").cast("float"))
            .agg(
                round(avg(col("THIRD_PARTY_READINESS_SCORE_FLOAT")), 2)
                .alias("AVERAGE_READINESS")
            )
            .to_pandas()
            .AVERAGE_READINESS
        )
    if result is None or result[0] is None or math.isnan(result[0]) :
        return 0
    else:
        return float(result)