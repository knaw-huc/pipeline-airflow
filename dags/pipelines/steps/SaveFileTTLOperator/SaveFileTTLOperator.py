import logging
from airflow.models import BaseOperator
# from utils import get_step_names

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
        if not previous_step:
            self.logger.error("Nothing to save, no previous step found in context")
            raise ValueError("Nothing to save, previous step not found in context")

        ttl_data = context['ti'].xcom_pull(task_ids=None, key=f"{previous_step.task_id}_{self.output_trace}")
        self.logger.info(f"Retrieved TTL data from XCom with key: {previous_step.task_id}_{self.output_trace}")
        self.logger.info(f"TTL data length: {len(ttl_data) if ttl_data else 'None'}")
        if not ttl_data:
            self.logger.error("No TTL data found in XCom")
            raise FileNotFoundError("TTL data not found in XCom")

        if self.output_trace:
            context['ti'].xcom_push(key=f"{step_names.get("current_step").task_id}_{self.output_store}", value=ttl_data)
            self.logger.info("TTL data pushed to XCom successfully")

        if self.output_store:
            with open(self.output_store, "w", encoding="utf-8") as f:
                f.write(ttl_data)
            self.logger.info(f"TTL data saved to {self.output_store}")

        return ttl_data