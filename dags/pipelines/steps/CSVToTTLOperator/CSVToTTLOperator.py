import re
import os
import logging
import argparse
import pandas as pd
from airflow.models import BaseOperator
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF
from utils import get_step_names


def csv_to_ttl(csv_file, ttl_file, base_uri, logger):
    df = pd.read_csv(csv_file)
    g = Graph()
    # Ensure base_uri does not end with a slash
    base_uri = f"{base_uri}" if base_uri.endswith("/") else base_uri
    # While NS has a trailing slash
    NS = Namespace(f"{base_uri}/")
    current_object = base_uri.split("/")[-1]

    for idx, row in df.iterrows():
        subject = df[current_object][idx]
        if subject.startswith("<http"):
            matches = re.findall(r'<(.*?)>', subject)
            if matches:
                subject = matches[0]
        subject = URIRef(subject)

        for col, val in row.items():
            if pd.notna(val):
                if col == current_object:
                    predicate = RDF.type
                    g.add((subject, predicate, URIRef(f"{base_uri}")))
                else:
                    predicate = NS[col]
                    g.add((subject, predicate, Literal(val)))

    with open(ttl_file, 'w') as f:
        f.write(g.serialize(format='ttl'))
    print(f"RDF data has been saved to {ttl_file}")


class CSVToTTLOperator(BaseOperator):
    def __init__(self, base_uri, **kwargs):
        super().__init__(**kwargs)
        self.base_uri = base_uri
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key='previous_output')
        if input_data and isinstance(input_data, dict) and "csv" in input_data:
            self.logger.info(f"Step: {self.task_id}, Converting csv row to ttl")
            csv_path = input_data["csv"]
            ttl_path = f"/tmp/{self.task_id}.ttl"
            csv_to_ttl(csv_path, ttl_path, self.base_uri, self.logger)
            input_data["ttl"] = ttl_path
            context["ti"].xcom_push("previous_output", input_data)
            return input_data
        else:
            self.logger.info("input data is None")
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV to TTL")
    parser.add_argument("-i", "--input", required=True, help="Input CSV file")
    parser.add_argument("-o", "--output", required=True, help="Output TTL file")
    parser.add_argument("-uri", "--base_uri", required=True, help="Base URI as a string")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if not args.input or not os.path.isfile(args.input):
        raise FileNotFoundError(f"Input file {args.input} does not exist.")
    if not args.output:
        raise ValueError("Output file path must be provided and cannot be empty.")
    else:
        output_dir = os.path.dirname(args.output)
        if not output_dir or not os.path.exists(output_dir):
            raise FileNotFoundError(f"Output directory {output_dir} does not exist.")
    if not args.ase_uri or not args.base_uri:
        raise ValueError("Base URI must be provided and cannot be empty.")
    if not args.base_uri.startswith("http://") and not args.base_uri.startswith("https://"):
        raise ValueError("Base URI must start with 'http://' or 'https://'.")

    csv_to_ttl(args.input, args.output, args.base_uri, logging.getLogger(__name__))
