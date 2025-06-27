import logging
from airflow.models import BaseOperator
from utils import get_step_names

class PrintOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        if input_data:
            self.logger.info(f"Input data received: {input_data}")
            context["ti"].xcom_push("previous_output", input_data)
            return input_data
        else:
            self.logger.info("input data is None")
            return None