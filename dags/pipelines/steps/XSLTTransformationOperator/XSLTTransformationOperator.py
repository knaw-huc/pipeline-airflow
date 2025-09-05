import os
import csv
import logging
import tempfile
from airflow.models import BaseOperator
from utils import get_step_names
from saxonche import PySaxonProcessor

class XSLTTransformationOperator(BaseOperator):
    def __init__(self, xslt_file: str = None,
                 fields_file: str | None = None,
                 output_trace: str | None = None,
                 output_store: str | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.xslt_file = xslt_file
        self.fields_file = fields_file
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        step_names: dict = get_step_names(context)
        self.logger.info(f"Current step: {step_names.get('current_step').task_id}")
        self.logger.info(f"xslt_file: {self.xslt_file}; fields_file: {self.fields_file}")

        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        if input_data:
            self.logger.info(f"Input data received: {input_data}")
            # Write input_data to a temporary CSV file if set
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_csv:
                temp_csv.write(input_data)
                temp_csv_path = temp_csv.name
            self.logger.info(f"Temporary CSV file created at: {temp_csv_path}")

            with PySaxonProcessor(license=False) as proc:
                xsltproc = proc.new_xslt30_processor()
                xslt_doc = proc.parse_xml(xml_uri=self.xslt_file)
                xsltproc.set_cwd(os.getcwd())
                executable = xsltproc.compile_stylesheet(stylesheet_node=xslt_doc)
                executable.set_parameter("csv", proc.make_string_value(f"file:{temp_csv_path}"))
                executable.set_parameter("out", proc.make_string_value(f"file:/tmp/{self.task_id}"))
                fields_doc = proc.parse_xml(xml_uri=self.fields_file)
                executable.set_global_context_item(xdm_item=fields_doc)
                res = executable.call_template_returning_string("main")

            context["ti"].xcom_push("previous_output", res)
            return res
        else:
            self.logger.info("input data is None")
            return None
