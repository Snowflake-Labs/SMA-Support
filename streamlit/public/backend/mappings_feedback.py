from public.backend.globals import *
import streamlit as st
import public.backend.app_snowpark_utils as utils
import pandas as pd
import json

def submitMappingsFeedback(df: pd.DataFrame, elementColumn):
    df = json.loads(df.to_json(orient='records'))
    userEmail = str(st.experimental_user.email)
    session = utils.get_session()
    jiraIds = session.call(SUBMIT_MAPPINGS_FEEDBACK_PROC, df, elementColumn, userEmail, KEY_JIRA_ASSIGNEE, KEY_JIRA_COMPONENT_DS_TOOL)
    return json.loads(jiraIds.split('_')[0].replace("'",'"')), json.loads(jiraIds.split('_')[1].replace("'",'"'))

def getExistingFeedback(df: pd.DataFrame, elementColumn):
    #df = json.loads(df.to_json(orient='records'))
    #session = utils.get_session()
    #data = session.call(f"{ENV_SNOW_DATABASE_DEFAULT_VALUE}.{ENV_SNOW_SCHEMA_DEFAULT_VALUE}.{GET_EXISTING_FFEDBACK}", df, elementColumn)
    data = json.loads('{"EXISTING FEEDBACK": [],"MAPPING COMMENT": [],"SPARK FULLY QUALIFIED NAME": []}')
    data = {FRIENDLY_NAME_MAPPING_COMMENTS: data[FRIENDLY_NAME_MAPPING_COMMENTS], 
            FRIENDLY_NAME_EXISTING_FEEDBACK: data[FRIENDLY_NAME_EXISTING_FEEDBACK],
            elementColumn: data[elementColumn]}
    df = pd.DataFrame(data)
    df[FRIENDLY_NAME_MAPPING_COMMENTS] = df[FRIENDLY_NAME_MAPPING_COMMENTS].astype("str")   
    return df