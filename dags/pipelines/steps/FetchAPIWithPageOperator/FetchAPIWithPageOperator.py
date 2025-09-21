import os
import re
import json
import httpx
import logging
import argparse
from rdflib import Graph
from urllib.parse import urlparse
from airflow.models import BaseOperator
from typing import Dict, List, Any, Union, LiteralString
from .config import config
from utils import get_step_names

# TODO: FIX error in adding context to JSON-LD, all the fields are missing now


# Setup logging
logging.basicConfig(
    level=config["logLevel"].upper(),
    format='%(asctime)s [%(levelname)s]: %(message)s',
    handlers=[logging.FileHandler(config["logFile"]), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Global HTTPX client
http_client = httpx.Client(follow_redirects=True, timeout=httpx.Timeout(10.0))
httpx_log = logging.getLogger("httpx")
httpx_log.setLevel(logging.ERROR)

table_map = {
    "ccode": "countrycode",
    "lifespan": "timespan",
    "part_of": "locationpartof",
    "external_id": "externalid",
    "child_location": "location",
    "parent_location": "location",
}


# def get_step_names(context):
#     current_step = context['task']
#     previous_steps = [task for task in context['task'].upstream_list]
#     return {
#         "current_step": current_step,
#         "previous_steps": previous_steps
#     }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch related tables starting from a given table from Globalise API in JSON-LD and Turtle")
    parser.add_argument('-t', '--tableName', type=str, help='Name of the table', default="location")
    parser.add_argument('-d', '--distance', type=int, help='Distance from the given table', default=3)
    return parser.parse_args()


def is_valid_uri(uri: str) -> bool:
    try:
        result = urlparse(uri)
        # Check for scheme and netloc for URLs, or at least scheme for URIs
        return all([result.scheme, result.netloc or result.path])
    except Exception:
        return False


def join_url(base: str, *paths: str) -> str:
    """Safely join URL parts"""
    return "/".join([base.rstrip("/")] + [str(p).strip("/") for p in paths if p is not None])


# JSON-LD context manipulation
def init_json_ld() -> Dict:
    return {
        "@context": {
            "@vocab": config["context"]["baseURI"],
            "id": "@id",
            "type": "@type"
        },
        "@graph": []
    }


def add_table_fields_to_context(json_ld: Dict, table_name: str, fields: Dict, table_prefix: str = "") -> Dict:
    context = json_ld["@context"]
    table_name_with_prefix = f"{table_prefix}{table_name}" if table_prefix else table_name

    context[table_name] = join_url(config["context"]["baseURI"], table_name_with_prefix)

    for field in fields:
        if field not in config["context"]["uniqueField"]:
            if field in table_map.keys():
                logger.debug(f"Mapping field {field} to {table_map[field]}")
                # special mapping case for child_location and parent_location to location
                if field in ["child_location", "parent_location"]:
                    context[f"{table_name}-{field}"] = join_url(config["context"]["baseURI"], table_name_with_prefix,
                                                                field)
                else:
                    context[f"{table_name}-{table_map[field]}"] = join_url(config["context"]["baseURI"],
                                                                           table_name_with_prefix, table_map[field])
            else:
                context[f"{table_name}-{field}"] = join_url(config["context"]["baseURI"], table_name_with_prefix, field)
    json_ld["@context"] = context
    return json_ld


def is_value_in_json(value: Union[str, int, bool], data: Any) -> bool:
    if data is None or not isinstance(data, dict):
        return False

    for key in data:
        if data[key] == value:
            return True

        if isinstance(data[key], dict):
            if is_value_in_json(value, data[key]):
                return True

    return False


def search_json_for_value(data, filter_dict, path=""):
    """
    Recursively search for the path to an object in data that matches the filter_dict.
    filter_dict should be a dict of key-value pairs to match.
    Returns the path as a string, or None if not found.
    """
    if isinstance(data, dict):
        # Check if all filter_dict items match in this dict
        if all(data.get(k) == v for k, v in filter_dict.items()):
            return path
        for k, v in data.items():
            sub_path = f"{path}.{k}" if path else k
            result = search_json_for_value(v, filter_dict, sub_path)
            if result is not None:
                return result
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            sub_path = f"{path}[{idx}]" if path else f"[{idx}]"
            result = search_json_for_value(item, filter_dict, sub_path)
            if result is not None:
                return result
    return None


def get_object_from_path(data: Any, path: str) -> Any:
    current = data
    parent = None
    # Split by dot, but keep list indices together
    parts = re.split(r'\.(?![^\[]*\])', path)
    for part in parts:
        parent = current
        # Handle list index, e.g., key[123]
        match = re.match(r'([^\[]+)(\[(\d+)\])?', part)
        if not match:
            return None
        key = match.group(1)
        idx = match.group(3)
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
        if idx is not None:
            if isinstance(current, list):
                i = int(idx)
                if 0 <= i < len(current):
                    parent = current
                    current = current[i]
                else:
                    return None
            else:
                return None
    return current


def extract_ns_prefixes(turtle_str: str) -> dict:
    """
    Removes lines starting with '@prefix ns' and returns a dict of nsX: URI mappings.
    """
    ns_dict = {}
    lines = turtle_str.splitlines()
    pattern = re.compile(r'^@prefix\s+(ns\d+):\s+<([^>]+)>')
    for line in lines:
        match = pattern.match(line)
        if match:
            ns_dict[match.group(1)] = match.group(2)
    return ns_dict


def remove_ns_prefixes(turtle_str: str) -> str:
    """
    Removes lines starting with '@prefix ns' from the Turtle string.
    """
    lines = turtle_str.splitlines()
    return "\n".join(line for line in lines if not line.startswith("@prefix ns"))


def expand_ttl(turtle: str, prefix_map: dict) -> str:
    # Replace nsX:something only when not inside <...>
    def replacer(match):
        prefix, local = match.group(1), match.group(2)
        # logger.info(f"Expanding {prefix}: {prefix_map.get(prefix)} with local part: {local}")
        if prefix in prefix_map:
            return f'<{prefix_map[prefix]}{local}>'
        return match.group(0)

    # Match nsX:something not preceded by < or inside <...>
    pattern = re.compile(r'(?<!<)(\bns\d+):([A-Za-z0-9_-]+)\b')
    return pattern.sub(replacer, turtle)


def convert_json_ld_to_ttl(json_ld: Dict) -> str:
    try:
        # Create an RDF graph
        graph = Graph()

        # Parse the JSON-LD into the graph
        graph.parse(data=json.dumps(json_ld), format="json-ld")

        # Serialize the graph to Turtle format
        ttl_str = graph.serialize(format="turtle")
        ns_prefixes: dict = extract_ns_prefixes(ttl_str)
        ttl_str = expand_ttl(ttl_str, ns_prefixes)
        ttl_str = remove_ns_prefixes(ttl_str)
        return ttl_str
    except Exception as e:
        logger.error(f"Error converting JSON-LD to TTL: {e}")
        raise


def validate_ttl(ttl: str) -> bool:
    try:
        graph = Graph()
        graph.parse(data=ttl, format="turtle")  # Use the `data` argument directly in `Graph.parse()`
        return True
    except Exception as e:
        logger.error(f"Invalid TTL data: {e}")
        return False


def save_json_ld_to_file(json_ld: Dict, file_path: LiteralString) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(json_ld, f, indent=2)


def get_all_endpoints() -> Dict:
    response = http_client.get(config["api"]["baseURL"])
    logger.info(f"Fetching endpoints from {config['api']['baseURL']}")
    if response.status_code != 200:
        raise Exception(f"Error fetching endpoints: {response.status_code}")

    tables = response.json()

    for stop_table in config["context"]["stopTables"]:
        tables.pop(stop_table, None)

    return tables


def fetch_record_by_id(table_name: str, record_id: str) -> Dict:
    url = join_url(config["api"]["baseURL"], table_name.lower(), record_id)
    response = http_client.get(url)
    if response.status_code != 200:
        raise Exception(f"Error fetching data from {url}: {response.status_code}")
    return response.json()


def fetch_table_in_batch(table_name: str, page: int = 1, page_size: int = 1) -> Dict:
    url = join_url(config["api"]["baseURL"], table_name.lower())
    params = {"page": page, "page_size": page_size}

    response = http_client.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching {table_name} from {url}: {response.status_code}")
    return response.json()


def fetch_table_rows(table_name: str) -> List:
    data = []
    next_url = join_url(config["api"]["baseURL"], table_name.lower())

    while next_url:
        logger.debug(f"Fetching data from: {next_url}")
        response = http_client.get(next_url)
        if response.status_code != 200:
            raise Exception(f"Error fetching data from {next_url}: {response.status_code}")

        result = response.json()
        data.extend(result.get("results", []))
        next_url = result.get("links", {}).get("next")

    return data


def fetch_table_metadata(table_name: str) -> Dict:
    result = fetch_table_in_batch(table_name, 1, 1)
    return result


def fetch_table(table_name: str) -> Dict:
    metadata = fetch_table_metadata(table_name)
    data = fetch_table_rows(table_name)
    return {
        "metadata": metadata,
        "data": data,
        "linkedTable": []
    }


# Main functionality
def replace_table_name(table_name: str) -> str:
    if table_name in table_map:
        logger.info(f"Changing table name from {table_name} to {table_map[table_name]}")
        return table_map[table_name]

    return table_name


def get_related_tables(table_name: str, tables: Dict) -> Dict:
    related_tables = {table_name: {"incoming": [], "outgoing": []}}

    # Add outgoing foreign keys
    outgoing: List[str] = []
    table_metadata = fetch_table_metadata(table_name)

    for related_table in table_metadata.get("metadata", {}).get("foreign_keys", {}).keys():
        outgoing.append(related_table)
    related_tables[table_name]["outgoing"] = outgoing

    # Add incoming foreign keys
    incoming: List[str] = []
    for table in tables:
        table_metadata = fetch_table_metadata(table)
        if table_name in table_metadata.get("metadata", {}).get("foreign_keys", {}).keys():
            incoming.append(table)
    related_tables[table_name]["incoming"] = incoming

    return related_tables


def get_related_tables_with_distance(
        table_name: str,
        tables: Dict,
        distance: int,
        related_tables: Dict = None
) -> Dict:
    if related_tables is None:
        related_tables = {}

    table_name = replace_table_name(table_name)

    if distance < 0:
        return related_tables

    current_related_tables = get_related_tables(table_name, tables)
    logger.info(f"Current related tables: {json.dumps(current_related_tables, indent=2)}")
    related_tables[table_name] = current_related_tables[table_name]
    logger.info(f"Current outgoing: {current_related_tables[table_name].get('outgoing', [])}")

    for related_table in current_related_tables[table_name].get("incoming", []) + current_related_tables[
        table_name].get("outgoing", []):
        if related_table not in related_tables:
            get_related_tables_with_distance(related_table, tables, distance - 1, related_tables)

    return related_tables


def _add_each_field(key, record_data, table_name_with_prefix, table_name, record, is_outgoing):
    if table_name == "location" and key == "point":
        # Special handling for 'point' field in 'location' table
        record_data[
            f"{table_name_with_prefix}-{key}"] = f"POINT({record[key]["coordinates"][0]},{record[key]["coordinates"][1]})"
    else:
        if key in table_map.keys():
            logger.debug(f"Mapping key {key} to {table_map[key]}")
            if is_outgoing:
                if table_name in ["locationpartof"] and key in ["child_location", "parent_location"]:
                    # special mapping case for child_location and parent_location to location
                    record_data[f"{table_name_with_prefix}-{key}"] = (
                        {"@id": join_url(config["context"]["baseURI"], table_map[key], record[key])}
                    )
                else:
                    record_data[f"{table_name_with_prefix}-{table_map[key]}"] = (
                        {"@id": join_url(config["context"]["baseURI"], table_map[key], record[key])}
                    )
            elif is_valid_uri(record[key]):
                if table_name in ["locationpartof"] and key in ["child_location", "parent_location"]:
                    # special mapping case for child_location and parent_location to location
                    record_data[f"{table_name_with_prefix}-{key}"] = (
                        {"@id": record[key]}
                    )
                else:
                    record_data[f"{table_name_with_prefix}-{table_map[key]}"] = (
                        {"@id": record[key]}
                    )
            else:
                if table_name in ["locationpartof"] and key in ["child_location", "parent_location"]:
                    # special mapping case for child_location and parent_location to location
                    record_data[f"{table_name_with_prefix}-{key}"] = record[key]
                else:
                    record_data[f"{table_name_with_prefix}-{table_map[key]}"] = record[key]
        else:
            if is_outgoing:
                record_data[f"{table_name_with_prefix}-{key}"] = (
                    {"@id": join_url(config["context"]["baseURI"], key, record[key])})
            elif is_valid_uri(record[key]):
                record_data[f"{table_name_with_prefix}-{key}"] = (
                    {"@id": record[key]})
            else:
                record_data[f"{table_name_with_prefix}-{key}"] = record[key]
    return record_data


# Record processing
def add_record_to_graph(
        json_ld: Dict,
        table_name: str,
        related_tables: Dict,
        record: Dict,
        table_prefix: str = "",
        check_linkage: bool = True
) -> Dict:
    graph = json_ld["@graph"]
    table_name_with_prefix = f"{table_prefix}{table_name}" if table_prefix else table_name
    record_id = record.get("id")

    record_data = {
        "@id": join_url(config["context"]["baseURI"], table_name_with_prefix, record_id),
        "@type": table_name
    }

    if table_name not in config["context"]["middleTables"]:
        for key in record:
            if not check_linkage or is_value_in_json(record_data["@id"], related_tables):
                if key not in config["context"]["uniqueField"] and record[key]:
                    if record[key] is None or (isinstance(record[key], str) and record[key].strip() == ""):
                        continue
                    is_outgoing = related_tables[table_name].get("outgoing", []) and key in related_tables[table_name][
                        "outgoing"]
                    record_data = _add_each_field(key, record_data, table_name_with_prefix, table_name, record,
                                                  is_outgoing)
    else:
        for key in record:
            if key not in config["context"]["uniqueField"]:
                if record[key] is None or (isinstance(record[key], str) and record[key].strip() == ""):
                    continue
                is_outgoing = related_tables[table_name].get("outgoing", []) and key in related_tables[table_name][
                    "outgoing"]
                try:
                    record_data = _add_each_field(key, record_data, table_name_with_prefix, table_name, record,
                                                  is_outgoing)
                except Exception as e:
                    logger.error(
                        f"Error adding record to graph: {e} {config['context']['baseURI']} {key} {json.dumps(record, indent=2)}")
                    raise

    if "records" not in related_tables[table_name]:
        related_tables[table_name]["records"] = []

    related_tables[table_name]["records"].append(record_data["@id"])
    graph.append(record_data)
    json_ld["@graph"] = graph
    return json_ld


def main(table_name: str, distance: int = 3):
    if not table_name:
        logger.error("No table name provided")
        return

    try:
        tables = get_all_endpoints()
        related_tables = get_related_tables_with_distance(table_name, tables, distance)

        logger.info(f"Working on {len(related_tables)} related tables out of {len(tables)} with distance {distance}")
        logger.debug(json.dumps(related_tables, indent=2))
        logger.info("Adding to graph")

        json_ld = init_json_ld()
        cache_related_tables = {}

        # Pre-fetch all related tables
        logger.info("Pre-fetching related tables")
        for related_table_name in related_tables:
            if related_table_name not in cache_related_tables:
                logger.info(f"Caching related table: '{related_table_name}'")
                cache_related_tables[related_table_name] = fetch_table(related_table_name)

        # Process main tables
        logger.info("Processing main tables")
        for related_table_name in related_tables:
            if related_table_name in config["context"]["mainEntryTables"]:
                logger.info(f"Processing main table: '{related_table_name}'")
                table = cache_related_tables[related_table_name]
                json_ld = add_table_fields_to_context(json_ld, related_table_name,
                                                      table["metadata"]["metadata"].get("fields", {}))

                for record in table["data"]:
                    json_ld = add_record_to_graph(json_ld, related_table_name, related_tables, record, "", False)

        # Process resource tables
        logger.info("Processing resource tables")
        for related_table_name in related_tables:
            if (related_table_name not in config["context"]["mainEntryTables"] and
                    related_table_name not in config["context"]["stopTables"] and
                    related_table_name not in config["context"]["middleTables"]):
                logger.info(f"Processing resource table: '{related_table_name}'")
                table = cache_related_tables[related_table_name]
                json_ld = add_table_fields_to_context(json_ld, related_table_name,
                                                      table["metadata"]["metadata"].get("fields", {}))

                for record in table["data"]:
                    json_ld = add_record_to_graph(json_ld, related_table_name, related_tables, record, "", False)

        # Process middle tables
        for related_table_name in related_tables:
            if related_table_name in config["context"]["middleTables"]:
                logger.info(f"Processing middle table: '{related_table_name}'")
                table = cache_related_tables[related_table_name]
                json_ld = add_table_fields_to_context(json_ld, related_table_name,
                                                      table["metadata"]["metadata"].get("fields", {}))

                for record in table["data"]:
                    json_ld = add_record_to_graph(json_ld, related_table_name, related_tables, record)

        # Save results
        output_dir = config["outputDir"]
        os.makedirs(output_dir, exist_ok=True)

        output_json_path: LiteralString = os.path.join(output_dir, config["outputJsonLd"])
        save_json_ld_to_file(json_ld, output_json_path)

        try:
            turtle = convert_json_ld_to_ttl(json_ld)

            if validate_ttl(turtle):
                output_ttl_path = os.path.join(output_dir, config["outputRdf"])
                with open(output_ttl_path, "w", encoding="utf-8") as f:
                    f.write(turtle)
                logger.info(f"Turtle data saved to {output_ttl_path}")
                logger.info(f"Turtle successfully converted from JSON-LD")
                return turtle
            else:
                logger.error("TTL is not valid")
                return
        except Exception as e:
            logger.error(f"Error during conversion to TTL: {e}")
            return

        logger.info("Done")
    finally:
        http_client.close()


class FetchAPIWithPageOperator(BaseOperator):
    def __init__(self, table_name: str, distance: int, output_trace: str, output_store: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.distance = distance
        self.output_trace = output_trace
        self.output_store = output_store
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        step_names: dict = get_step_names(context)
        # Run the main function
        ttl_data = main(self.table_name, self.distance)
        # Push the output to XCom
        if self.output_trace:
            context['ti'].xcom_push(key=f"{step_names.get("current_step").task_id}_{self.output_store}", value=ttl_data)
            self.logger.info("TTL data pushed to XCom successfully")
        if self.output_store:
            with open(f"{step_names.get("current_step").task_id}.{self.output_store}", "w", encoding="utf-8") as f:
                f.write(ttl_data)
            self.logger.info(f"TTL data saved to {step_names.get('current_step').task_id}.{self.output_store}")
        return ttl_data


if __name__ == "__main__":
    args = parse_args()
    table_name = args.tableName
    distance: int = args.distance
    main(table_name, distance)
