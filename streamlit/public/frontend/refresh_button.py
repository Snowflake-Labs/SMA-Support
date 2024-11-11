import streamlit as st
from public.backend.app_snowpark_utils import can_refresh
import public.backend.common_backend as backend
from public.backend.app_snowpark_utils import last_refresh
import public.backend.telemetry as telemetry
import time
from public.backend.globals import *
import public.frontend.error_handling as errorHandling

@errorHandling.executeFunctionWithErrorHandling
def refresh_button():
  if st.button("Reload executions", key="refreshButton", help="Load the latest executions available."):
    refresh_is_enable = can_refresh(last_refresh)
    if refresh_is_enable:
      with st.spinner("Running refresh procedure..."):
        backend.callProcedureNoArgs("REFRESH_PORTAL_CACHES")
      st.success("Latest executions loaded.")
      telemetry.logTelemetry(CLICK_RELOAD_EXECUTIONS)
      time.sleep(2)
      st.rerun()
    if not refresh_is_enable:
      st.info("Please wait a couple of minutes to reload.")