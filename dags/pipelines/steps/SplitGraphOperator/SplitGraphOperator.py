import os
import logging
from pathlib import Path
from rdflib import Graph, URIRef
from airflow.models import BaseOperator
from utils import get_step_names


class SplitGraphOperator(BaseOperator):
    def __init__(self, main_place_graph, places_query, **kwargs):
        super().__init__(**kwargs)
        self.main_place_graph = main_place_graph
        self.logger = logging.getLogger(__name__)

        # Query all places
        self.places_query = places_query


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

    def execute(self, context):
        # Create a new graph
        main_graph = Graph()
        main_graph.parse(self.main_place_graph, format="turtle")
        self.logger.info(f"Main graph has {len(main_graph)} triples")

        # Process each place
        counter = 1
        result = []
        for row in main_graph.query(self.places_query):
            self.logger.info(f"Processing place {counter}")
            counter += 1
            place_uri = row.place

            self.logger.info(f"Filter value: {place_uri}")

            # Create a new graph for this place
            place_graph = Graph()

            # Add all related triples
            self.add_related_triples(place_graph, place_uri, main_graph)

            # Generate filename from URI
            filename = place_uri.split('/')[-1] + ".ttl"
            output_path = os.path.join("/tmp/", filename)

            # Save to file
            place_graph.serialize(destination=str(output_path), format="turtle")
            result.append(output_path)
        return result