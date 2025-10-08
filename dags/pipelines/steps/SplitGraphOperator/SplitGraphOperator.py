import os
import json
import logging
from pathlib import Path
from rdflib import Graph, URIRef
from airflow.models import BaseOperator
from utils import get_step_names


class SplitGraphOperator(BaseOperator):
    def __init__(self, message_queue, output_trace, output_store, **kwargs):
        super().__init__(**kwargs)
        self.message_queue = message_queue
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def add_related_triples(self, graph, entity, main_graph, processed_entities=None):
        if processed_entities is None:
            processed_entities = set()

        if entity in processed_entities:
            return

        processed_entities.add(entity)

        # Add all triples where entity is subject
        for p, o in main_graph.predicate_objects(entity):
            graph.add((entity, p, o))
            # Add all triples about the object if it's a URI
            if isinstance(o, URIRef):
                for p2, o2 in main_graph.predicate_objects(o):
                    graph.add((o, p2, o2))

        # Add all triples where entity is object
        for s, p in main_graph.subject_predicates(entity):
            graph.add((s, p, entity))
            # Add all triples about the subject if it's a URI
            if isinstance(s, URIRef):
                for p2, o2 in main_graph.predicate_objects(s):
                    graph.add((s, p2, o2))

    def get_place_id(self, ttl):
        # given a ttl string, return the place id
        for line in ttl.splitlines():
            if "a <http://www.cidoc-crm.org/cidoc-crm/E53_Place>" in line:
                return line.split()[0].strip("<>")
        return None

    def get_filename_from_id(self, place_id):
        # given a place id, return the filename
        if place_id:
            return place_id.split('/')[-1] + ".ttl"
        raise ValueError("Place missing or invalid place id")

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key=self.message_queue)

        # Process each place
        counter = 0
        result = {}
        for row in input_data:
            counter += 1
            self.logger.info(f"Processing place {counter}")
            self.logger.info(f"Task id: {self.task_id}: {json.dumps(row, indent=2)}")
            ttl_string = row.get("result", "")
            ttl_file_path = row.get("ttl", "")
            place_id = self.get_place_id(ttl_string)
            output_filename = self.get_filename_from_id(place_id)
            output_path = os.path.join("/tmp/", f"{self.task_id}_{output_filename}")
            self.logger.info(f"Place id: {place_id}")
            self.logger.info(f"Generated filename: {output_filename}")

            graph = Graph()
            if place_id in result.keys():
                graph.parse(output_path, format="turtle")
            else:
                result[place_id] = output_path

            graph.parse(data=ttl_string, format="turtle")
            graph.serialize(destination=output_path, format="turtle")

        return result