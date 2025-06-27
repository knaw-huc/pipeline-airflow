import os
import logging
import subprocess
from airflow.models import BaseOperator
from utils import get_step_names


class RunSparqlComunicaOperator(BaseOperator):
    def __init__(self, docker_image: str, docker_network: str, docker_rdf_file: str, docker_output_format: str,
                 query: str, output_store: str = None, output_trace: str = None, **kwargs):
        super().__init__(**kwargs)
        self.docker_image = docker_image
        self.docker_network = docker_network
        self.docker_rdf_file = docker_rdf_file
        self.docker_output_format = docker_output_format
        self.query = query
        self.output_store = output_store
        self.output_trace = output_trace
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        self.logger.info("Running SPARQL query using Comunica Docker image...")
        step_names = get_step_names(context)

        # Prepare the Docker command
        command = [
            "docker", "run", "--rm",
            "--network", self.docker_network,
            "-v", "sample_data_vol:/tmp",
            self.docker_image,
            self.docker_rdf_file,
        ]
        if os.path.isfile(self.query):
            command.extend(["-f", self.query])
        else:
            command.extend(["-q", self.query])

        if self.docker_output_format:
            command.extend(["-t", self.docker_output_format])

        self.logger.info(f"Executing command: {' '.join(command)}")

        try:
            # Run the Docker command
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            output = result.stdout
            self.logger.info("SPARQL query executed successfully.")
            self.logger.info(f"Query output: {output}")

            # Push the output to XCom if `output_trace` is specified
            if self.output_trace:
                context['ti'].xcom_push(key=f"{step_names.get("current_step").task_id}_{self.output_trace}",
                                        value=output)
                self.logger.info(f"Output pushed to XCom with key: {step_names.get("current_step").task_id}_{self.output_trace}")

            if self.output_store:
                output_file_path = f"{step_names.get('current_step').task_id}.{self.output_store}"
                with open(output_file_path, "w", encoding="utf-8") as f:
                    f.write(output)
                self.logger.info(f"Output saved to {output_file_path}")

            return output
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error while running SPARQL query: {e.stderr}")
            raise RuntimeError(f"SPARQL query execution failed: {e.stderr}")