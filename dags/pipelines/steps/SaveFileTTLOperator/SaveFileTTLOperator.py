import os
import logging
from airflow.models import BaseOperator

def get_step_names(context):
    current_step = context['task']
    previous_steps = [task for task in context['task'].upstream_list]
    return {
        "current_step": current_step,
        "previous_steps": previous_steps
    }

class SaveFileTTLOperator(BaseOperator):
    def __init__(self, task_id: str, dag, output_trace: str = "ttl", output_store: str = None, **kwargs):
        super().__init__(task_id=task_id, dag=dag, **kwargs)
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        step_names = get_step_names(context)
        previous_step = step_names.get("previous_steps", [])[-1] if step_names.get("previous_steps") else None
        return_value = context['ti'].xcom_pull(task_ids=None, key='return_value')

        ttl_data = None
        if not previous_step and not return_value:
            self.logger.error("Nothing to save, no previous step found in context")
            raise ValueError("Nothing to save, previous step not found in context")
        self.logger.info(f"Previous step: {previous_step}: Return value: {return_value}")
        if previous_step:
            ttl_data = context['ti'].xcom_pull(task_ids=None, key=f"{previous_step.task_id}_{self.output_trace}")
            self.logger.info(f"Retrieved TTL data from XCom with key: {previous_step.task_id}_{self.output_trace}")
            self.logger.info(f"TTL data length: {len(ttl_data) if ttl_data else 'None'}")
            if not ttl_data:
                self.logger.error("No TTL data found in previous step's XCom")

        if return_value and (not ttl_data or len(ttl_data) == 0) and isinstance(return_value, str) and os.path.isfile(return_value):
            self.logger.info(f"Reading from previous step failed; Trying TTL data from file: {return_value}")
            with open(return_value, "r", encoding="utf-8") as f:
                ttl_data = f.read()
            if not ttl_data:
                self.logger.error("No TTL data found in return_value")
                raise FileNotFoundError("TTL data not found in return_value")

        if self.output_trace:
            context['ti'].xcom_push(key=f"{step_names.get("current_step").task_id}_{self.output_store}", value=ttl_data)
            self.logger.info("TTL data pushed to XCom successfully")

        if self.output_store:
            with open(self.output_store, "w", encoding="utf-8") as f:
                f.write(ttl_data)
            self.logger.info(f"TTL data saved to {self.output_store}")

        return ttl_data
