import os
import csv
import logging
import tempfile
import shutil
from airflow.models import BaseOperator
from utils import get_step_names
from saxonche import PySaxonProcessor

class XSLTTransformationOperator(BaseOperator):
    def __init__(self, xslt_file: str = None,
                 fields_file: str | None = None,
                 output_trace: str | None = None,
                 output_store: str | None = None,
                 xslt_params: dict | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.xslt_file = xslt_file
        self.fields_file = fields_file
        self.output_trace = output_trace
        self.output_store = output_store
        self.xslt_params = xslt_params
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        self.logger.info(f"xslt_file: {self.xslt_file}; fields_file: {self.fields_file}")
        csv_output = f"/tmp/{self.task_id}.csv"
        sparql_output = f"/tmp/{self.task_id}.{self.output_store}"


        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        if input_data:
            self.logger.debug(f"Input data received: {input_data}")
            # Write input_data to a temporary CSV file if set
            with open(csv_output, 'w') as temp_csv:
            # with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_csv:
                temp_csv.write(input_data)
                csv_output = temp_csv.name
            self.logger.info(f"Temporary CSV file created at: {csv_output}")

            with PySaxonProcessor(license=False) as proc:
                xsltproc = proc.new_xslt30_processor()
                xslt_doc = proc.parse_xml(xml_uri=self.xslt_file)
                xsltproc.set_cwd(os.getcwd())
                executable = xsltproc.compile_stylesheet(stylesheet_node=xslt_doc)
                # setting calculated params
                executable.set_parameter("csv", proc.make_string_value(f"file:{csv_output}"))
                executable.set_parameter("out", proc.make_string_value(f"file:{sparql_output}"))
                # setting xslt_params
                self.logger.info(f"Setting params: {self.xslt_params}")
                for k, v in (self.xslt_params or {}).items():
                    self.logger.info(f"Setting param: {k}={v}")
                    executable.set_parameter(k, proc.make_string_value(v))
                # setting resource file
                fields_doc = proc.parse_xml(xml_uri=self.fields_file)
                executable.set_global_context_item(xdm_item=fields_doc)
                # run the transformation
                res = executable.call_template_returning_string("main")

            shutil.chown(sparql_output, user='airflow', group='root')
            os.chmod(sparql_output, 0o777)
            result = {"csv": csv_output, "sparql": sparql_output, "result": res}
            context["ti"].xcom_push("previous_output", result)
            return result
        else:
            self.logger.info("input data is None")
            return None
