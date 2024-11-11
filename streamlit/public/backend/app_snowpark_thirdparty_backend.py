import public.backend.app_snowpark_utils as utils
import streamlit as st
from snowflake.snowpark.functions import col, expr
from public.backend.query_quality_handler import with_table_quality_handler
from public.backend.globals import *

@st.cache_data(show_spinner=True)
def getAllThirdPartyUsagesCachedByExecutionId (executionIds):
    session = utils.get_session()
    category_table = with_table_quality_handler(session.table(TABLE_THIRD_PARTY_CATEGORIES), TABLE_THIRD_PARTY_CATEGORIES)
    third_party_table = with_table_quality_handler(session.table(VIEW_ALL_THIRD_PARTY_USAGES_CACHED), VIEW_ALL_THIRD_PARTY_USAGES_CACHED)
    subquery = third_party_table.join(category_table).filter((col(COLUMN_TOOL_EXECUTION_ID)
                .isin(executionIds)) & (expr(f"REGEXP_LIKE({COLUMN_ELEMENT},{COLUMN_FILTER})"))).select(col(COLUMN_ELEMENT).alias(f"C_{COLUMN_ELEMENT}"), col(COLUMN_CATEGORY)) \
                .distinct()
    result = (third_party_table.join(subquery, on=(
        third_party_table[COLUMN_ELEMENT] == subquery[f"C_{COLUMN_ELEMENT}"]), how="left")
        .filter(col(COLUMN_TOOL_EXECUTION_ID).isin(executionIds))
        ).drop(f"C_{COLUMN_ELEMENT}")

    return result.to_pandas()
