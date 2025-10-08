import os
import logging
import httpx
import tempfile
import subprocess
import shutil
from airflow.models import BaseOperator


def create_uri_from_file(file_path: str, input_data: dict) -> str | None:
    file_path = file_path.split(":", 1)[1]
    file_path = input_data.get(file_path, None)
    if file_path:
        return f"http://util-server:8000/static/{os.path.basename(file_path)}"
    return None





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

    def add_node_to_path(self, env, nvm_dir: str = "/home/airflow/.nvm/versions/node"):
        node_versions = sorted([d for d in os.listdir(nvm_dir) if d.startswith("v")], reverse=True)
        if not node_versions:
            raise RuntimeError("No Node.js version found in NVM directory")
        for node in node_versions:
            node_version = node.lstrip("v")
            node_path = f"{nvm_dir}/v{node_version}/bin"
            env["PATH"] = f"{node_path}:" + env["PATH"]
            self.logger.info(f"Adding Node.js version: {node_version} to PATH; PATH: {env['PATH']}")
        return env

    def execute(self, context):
        self.logger.info("Running SPARQL query ...")
        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        self.logger.debug(f"Input data: {input_data}")

        command = [
            "comunica-sparql",
        ]
        # rdf file
        if self.docker_rdf_file.startswith("file_uri:"):
            self.docker_rdf_file = create_uri_from_file(self.docker_rdf_file, input_data) or self.docker_rdf_file
        command.extend([self.docker_rdf_file])

        # sparql query
        if os.path.isfile(self.query):
            command.extend(["-f", self.query])
        elif self.query.startswith("http://") or self.query.startswith("https://") or self.query.startswith("file_uri:"):
            if self.query.startswith("file_uri:"):
                file_uri = create_uri_from_file(self.query, input_data)
                self.query = file_uri if file_uri else self.query
            self.logger.debug(f"Using file URI: {self.query}")
            # download the file
            with httpx.Client() as client:
                response = client.get(self.query)
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8",
                                                 suffix=".sparql") as tmp_file:
                    tmp_file.write(response.text)
                    tmp_file_path = tmp_file.name
            command.extend(["-f", tmp_file_path])
        else:
            command.extend(["-q", self.query])

        if self.docker_output_format:
            command.extend(["-t", self.docker_output_format])

        self.logger.info(f"Executing command: {' '.join(command)}")

        try:
            env = os.environ.copy()
            env = self.add_node_to_path(env)
            self.logger.debug(os.listdir("/tmp"))  # Ensure /tmp is accessible
            result = subprocess.run(command, capture_output=True, text=True, check=True, env=env)
            output = result.stdout
            self.logger.info("SPARQL query executed successfully.")
            self.logger.debug(f"Query output: {output}")

            # prepare return result
            result = {}

            # add output content to output trace if configured
            if self.output_trace:
                result["result"] = output

            # save output to file if configured
            if self.output_store:
                output_file_path = f"/tmp/{self.task_id}.{self.output_store}"
                with open(output_file_path, "w", encoding="utf-8") as f:
                    f.write(output)
                self.logger.info(f"Output saved to {output_file_path}")
                result[self.output_store] = output_file_path

            # push result to XCom
            if self.output_trace:
                context["ti"].xcom_push("previous_output", result)
            return result
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error while running SPARQL query: {e.stderr}")
            raise RuntimeError(f"SPARQL query execution failed: {e.stderr}")