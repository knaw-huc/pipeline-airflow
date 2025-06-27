import logging
import rdflib
import httpx
from airflow.models import BaseOperator
from rdflib import Graph


class RunSparqlComunicaOperator(BaseOperator):
    def __init__(self, ttl_file_path: str, query: str, output_format: str = "ttl", output_trace: str = "ttl", **kwargs):
        super().__init__(**kwargs)
        self.ttl_file_path = ttl_file_path
        self.query = query
        self.output_format = output_format
        self.output_trace = output_trace
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        import subprocess
        import threading
        import http.server
        import socketserver
        import os
        import time

        self.logger.info("Starting execution of RunSparqlComunicaOperator")
        self.logger.info(f"SPARQL query: {self.query}")
        self.logger.info(f"TTL file path: {self.ttl_file_path}")
        self.logger.info(f"Output format: {self.output_format}")

        # Serve the TTL file via a simple HTTP server
        class Handler(http.server.SimpleHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Suppress HTTP server logs

        dir_path = os.path.dirname(self.ttl_file_path)
        file_name = os.path.basename(self.ttl_file_path)
        port = 8081  # Use a fixed port for simplicity

        httpd = socketserver.TCPServer(("", port), Handler)
        os.chdir(dir_path)

        def serve():
            httpd.serve_forever()

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()
        self.logger.info(f"Serving {file_name} at http://localhost:{port}/{file_name}")

        # Wait a moment for the server to start
        time.sleep(1)

        # Run the Comunica Docker command
        ttl_url = f"http://localhost:{port}/{file_name}"
        docker_cmd = [
            "docker", "run", "-i", "--rm",
            "comunica/query-sparql",
            ttl_url,
            self.query,
            "-t", self.output_format
        ]
        self.logger.info(f"Running Docker command: {' '.join(docker_cmd)}")

        try:
            result = subprocess.check_output(docker_cmd, stderr=subprocess.STDOUT, text=True)
            self.logger.info("SPARQL query executed successfully")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to execute SPARQL query: {e.output}")
            httpd.shutdown()
            raise

        httpd.shutdown()
        self.logger.info("HTTP server shut down")

        # Push result to XCom if needed
        if self.output_trace == self.output_format:
            context['ti'].xcom_push(key="output_trace", value=result)
            self.logger.info(f"Result pushed to XCom successfully as {self.output_format}")

        return result
