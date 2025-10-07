import json
import logging
from rdflib import Graph
from pyld import jsonld
from airflow.models import BaseOperator
from utils import get_step_names


json_context = {
    "@context": [{
    "geo": "http://www.opengis.net/ont/geosparql#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "crm": "http://www.cidoc-crm.org/cidoc-crm/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",

    "aaao": "https://ontology.swissartresearch.net/aaao/",
    "sar": "https://example.swissartresearch.net/",
    "crmgeo": "http://www.ics.forth.gr/isl/CRMgeo/",
    "aat": "http://vocab.getty.edu/page/aat/",

    "is_appellative_subject_of": {
      "@id": "aaao:ZP5i_is_appellative_subject_of",
      "@type": "@id"
    },
    "ascribes_appellation": {
      "@id": "aaao:ZP6_ascribes_appellation",
      "@type": "@id"
    },
    "is_appellation_ascribed_by": {
      "@id": "aaao:ZP6i_is_appellation_ascribed_by",
      "@type": "@id"
    },
    "is_classificatory_subject_of": {
      "@id": "aaao:ZP11i_is_classificatory_subject_of",
      "@type": "@id"
    },
    "ascribes_classification": {
      "@id": "aaao:ZP12_ascribes_classification",
      "@type": "@id"
    },
    "ascribes_place": {
      "@id": "aaao:ZP77_ascribes_place",
      "@type": "@id"
    },
    "is_topographical_subject_of": {
      "@id": "aaao:ZP85i_is_topographical_subject_of",
      "@type": "@id"
    },

    "defined_by": {
      "@id": "crmgeo:Q10_defined_by",
      "@type": "@id"
    },
    "is_approximated_by": {
      "@id": "crmgeo:Q11i_is_approximated_by",
      "@type": "@id"
    },

    "AppellativeStatus": "aaao:ZE2_Appellative_Status",
    "ClassificatoryStatus": "aaao:ZE4_Classificatory_Status",
    "TopographicalStatus": "aaao:ZE44_Topographical_Status",

    "GeometricPlaceExpression": "crmgeo:SP5_Geometric_Place_Expression",
    "DeclarativePlace": "crmgeo:SP6_Declarative_Place"

  }, "https://linked.art/ns/v1/linked-art.json"]
}

class ConvertTtlToJsonldOperator(BaseOperator):
    def __init__(self, message_queue, output_trace, output_store, **kwargs):
        super().__init__(**kwargs)
        self.message_queue = message_queue
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def ttl_to_jsonld_advanced(self, ttl_file_path, output_file_path, context=None):
        # First convert to simple JSON-LD using RDFLib
        g = Graph()
        g.parse(ttl_file_path, format='turtle')
        jsonld_data = json.loads(g.serialize(format='json-ld'))

        # Apply context compaction if provided
        if context:
            compacted = jsonld.compact(jsonld_data, context)
            jsonld_data = compacted

        # Write to file
        with open(output_file_path, 'w') as f:
            json.dump(jsonld_data, f, indent=2)

        return jsonld_data

    def execute(self, context):
        input_data = context['ti'].xcom_pull(task_ids=None, key=self.message_queue)

        # Process each place
        counter = 0
        result = {}
        for k, v in input_data.items():
            counter += 1
            self.logger.info(f"Processing place {counter}")
            self.logger.info(f"Task id: {self.task_id}: {json.dumps(v, indent=4)}")

            output_path = v.replace(".ttl", ".json")

            self.ttl_to_jsonld_advanced(v, output_path, context=json_context)

            # graph = Graph()
            # with open(v, "r", encoding="utf-8") as f:
            #     ttl_string = f.read()
            # graph.parse(data=ttl_string, format="turtle")
            # graph.serialize(destination=output_path, format="json-ld", indent=4)
            result[k] = output_path
            self.logger.info(f"Serialized {v} to {output_path}")
        return result