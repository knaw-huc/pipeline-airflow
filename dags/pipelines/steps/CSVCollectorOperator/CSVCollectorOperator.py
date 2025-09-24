import logging
from airflow.models import BaseOperator

class CSVCollectorOperator(BaseOperator):
    def __init__(self, message_queue, **kwargs):
        super().__init__(**kwargs)
        self.message_queue = message_queue
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        if input_data:
            self.logger.info(f"One input data received: {input_data}")
            self.logger.info(f"Task id: {self.task_id}")
            result = context['ti'].xcom_pull(task_ids=None, key=f"{self.message_queue}")
            if not result:
                result = []
            result.append(input_data)
            context["ti"].xcom_push(key=f"{self.message_queue}", value=result)
            return {"message_queue": self.message_queue}
        else:
            self.logger.info("input data is None")
            return {"message_queue": None}