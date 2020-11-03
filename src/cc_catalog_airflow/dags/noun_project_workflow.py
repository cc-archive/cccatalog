"""
This file configures the Apache Airflow DAG to (re)ingest Noun Project data.
"""
# airflow DAG (necessary for Airflow to find this file)
from datetime import datetime, timedelta
import logging

from provider_api_scripts import noun_project as nounpro
from util.dag_factory import create_provider_api_workflow

logging.basicConfig(
    format='%(asctime)s: [%(levelname)s - DAG Loader] %(message)s',
    level=logging.DEBUG)


logger = logging.getLogger(__name__)

DAG_ID = 'noun_project_workflow'

globals()[DAG_ID] = create_provider_api_workflow(
    DAG_ID,
    nounpro.main,
    start_date=datetime(2020, 9, 27),
    schedule_string='@monthly',
    dated=False,
    dagrun_timeout=timedelta(days=1)
)
