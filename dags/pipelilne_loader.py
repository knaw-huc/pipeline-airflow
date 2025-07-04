import sys
import json
import yaml
import logging
import importlib
from pathlib import Path
from airflow import DAG
from datetime import datetime

# Add pipelines/steps to Python path
steps_path = Path(__file__).parent / "pipelines" / "steps"
sys.path.append(str(steps_path))

logger = logging.getLogger(__name__)


def validate_yaml(yaml_path: str, schema_path: str):
    with open(yaml_path) as f:
        yaml_data = yaml.safe_load(f)
    with open(schema_path) as f:
        schema = json.load(f)
    # jsonschema.validate(yaml_data, schema)
    return yaml_data


def load_pipeline(yaml_path: str):
    schema_path = str(Path(yaml_path).parent / "schema.json")
    if not Path(yaml_path).exists():
        raise FileNotFoundError(f"YAML file not found: {yaml_path}; current directory: {Path.cwd()}")
    config = validate_yaml(yaml_path, schema_path)

    dag = DAG(
        dag_id=config["pipeline"]["name"],
        description=config["pipeline"].get("description"),
        schedule_interval=config["pipeline"]["schedule"],
        start_date=datetime(2023, 1, 1),
        catchup=False,
        params={
            "logLevel": 'info',
            "logFile": "app.log",
            "outputDir": "output",
            "outputJsonLd": "output.jsonld",
            "outputRdf": "output.ttl",
            "api": {
                "baseURL": "http://localhost/api"
            },
            "context": {
                "baseURI": "http://example.globalise.nl/temp",
                "uniqueField": ["id", "@type"],
                "stopTables": ["logentry", "permission", "group", "user", "contenttype", "session",
                               "postgisgeometrycolumns",
                               "postgisspatialrefsys", "places"],
                "middleTables": ["timespan2source", "polity2source", "politylabel2source", " rulership2source",
                                 "rulershiplabel2source", "rulergender2source", "ruler2source", "rulerlabel2source",
                                 "reign2source", "location2countrycode", "location2coordsource", "location2source",
                                 "location2externalid", "location2type", "locationtype2source", "locationlabel2source",
                                 "locationpartof2source", "shiplabel2source", "ship2externalid", "ship2type",
                                 "ship2source", "event2source", "event2location", "translocation2externalid",
                                 "translocation2source", "translocation2location", "locationlabel"
                                 ],
                "mainEntryTables": ["polity", "politylabel", "reign", "ruler", "rulership", "rulershiplabel",
                                    "rulerlabel",
                                    "shiplabel", "location", "event", "translocation"],
                "notSure": ["locationpartof"]
            },
        }
    )

    tasks = {}
    previous_task = None
    for task_id, task_config in config["pipeline"]["tasks"].items():
        operator_type = task_config["type"]
        try:
            operator_module = importlib.import_module(operator_type)
            logger.info(f"Available attributes in {operator_module}: {dir(operator_module)}")
            operator_class = getattr(operator_module, operator_type)
        except AttributeError:
            # try:
            #     operator_module = importlib.import_module("custom_operators")
            #     logger.info(f"Available attributes in {operator_module}: {dir(operator_module)}")
            #     operator_class = getattr(operator_module, operator_type)
            # except AttributeError:
            #     raise ValueError(f"Operator class {operator_type} not found in steps or custom_operators")
            raise ValueError(f"Operator class {operator_type} not found in steps or custom_operators")

        task_config_filtered = {key: value for key, value in task_config.items() if key != "type"}
        op = operator_class(task_id=task_id, dag=dag, **task_config_filtered)
        tasks[task_id] = op

        # Set task dependencies
        if previous_task:
            previous_task >> op
        previous_task = op

    return dag


# Register the DAG
globals()["globalise_pipeline"] = load_pipeline("/opt/airflow/dags/pipelines/pipeline.yaml")
