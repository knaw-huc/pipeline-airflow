import logging
import rdflib
from airflow.models import BaseOperator

class TTLMergerOperator(BaseOperator):
    def __init__(self, message_queue, **kwargs):
        super().__init__(**kwargs)
        self.message_queue = message_queue
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key=f'{self.message_queue}')
        if input_data:
            merged_ttl = rdflib.Graph()
            self.logger.debug(f"One input data received: {input_data}")
            for result in input_data:
                for k, v in result.items():
                    if k == "ttl":
                        try:
                            with open(v, "r", encoding="utf-8") as fh:
                                merged_ttl.parse(file=fh, format="n3")
                            self.logger.info(f"Merging TTL file {v}: {len(merged_ttl)} triples")
                        except Exception as e:
                            self.logger.error(f"Error parsing TTL data: {e}")

            try:
                merged_ttl_data = merged_ttl.serialize(format="turtle")
                output_file = f"/tmp/{self.task_id}.ttl"
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(merged_ttl_data)
            except Exception as e:
                self.logger.error(f"Error serializing merged TTL data: {e}")
                raise e

            self.logger.info("Merged TTL data pushed to XCom successfully")
            return output_file
        else:
            self.logger.info("input data is None")
            return {"message_queue": None}