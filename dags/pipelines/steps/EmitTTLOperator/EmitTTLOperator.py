import logging
from airflow.models import BaseOperator
from rdflib import Graph
from utils import get_step_names

class EmitTTLOperator(BaseOperator):
    def __init__(self, input_file_path: str = None, output_trace: str = "ttl", output_store: str = "ttl", **kwargs):
        super().__init__(**kwargs)
        self.ttl_file_path = input_file_path
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        ttl_data = None
        step_names: dict = get_step_names(context)
        # Attempt to load TTL file if path is provided
        graph = Graph()
        if self.ttl_file_path:
            if self.ttl_file_path.startswith("http://") or self.ttl_file_path.startswith("https://"):
                self.logger.info(f"Fetching TTL file from URL: {self.ttl_file_path}")
                try:
                    graph.parse(self.ttl_file_path, format="turtle")
                    self.logger.info("TTL file loaded successfully from URL")
                except Exception as e:
                    raise ValueError(f"Found url, but failed to load URL: {e}. ")
            else:
                try:
                    graph.parse(self.ttl_file_path, format="turtle")
                    self.logger.info("TTL file loaded successfully")
                except Exception as e:
                    raise ValueError(f"Found file path, but failed to load TTL file: {e}. ")
            ttl_data = graph.serialize(format="turtle")
        else:
            # If TTL file is not valid, retrieve TTL data from XCom
            try:
                self.logger.info("Attempting to retrieve TTL data from XCom")
                ttl_data = context['ti'].xcom_pull(task_ids=None, key=self.output_trace)
            except Exception as e:
                raise ValueError(f"Failed to retrieve TTL data from XCom: {e}")

        if not ttl_data:
            self.logger.error("No TTL data found in XCom")
            raise FileNotFoundError("TTL file not found and no TTL data in XCom")

        # Push TTL data to XCom
        if self.output_trace:
            context['ti'].xcom_push(key=f"{step_names.get("current_step").task_id}_{self.output_store}", value=ttl_data)
            self.logger.info("TTL data pushed to XCom successfully")
        if self.output_store:
            with open(f"{step_names.get("current_step").task_id}.{self.output_store}", "w", encoding="utf-8") as f:
                f.write(ttl_data)
            self.logger.info(f"TTL data saved to {step_names.get('current_step').task_id}.{self.output_store}")
        return ttl_data
