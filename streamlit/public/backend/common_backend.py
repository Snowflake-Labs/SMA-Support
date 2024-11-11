from public.backend import app_snowpark_utils as utils


def callProcedureNoArgs(procedureName: str):
    session = utils.get_session()
    session.call(procedureName)
