import os
import json
import logging
import argparse
from typing import Dict, List, Any, Union
import httpx
from rdflib import Graph
from config import config

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


def parse_args():
    parser = argparse.ArgumentParser(description="Convert JSON-LD to TTL")
    parser.add_argument('-t', '--tableName', type=str, help='Name of the table', default="location")
    parser.add_argument('-d', '--distance', type=int, help='Distance from the given table', default=3)
    return parser.parse_args()


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

    if table_name not in config["context"]["middleTables"]:
        context[table_name] = join_url(config["context"]["baseURI"], table_name_with_prefix)

        for field in fields:
            if field not in config["context"]["uniqueField"]:
                context[f"{table_name}-{field}"] = join_url(config["context"]["baseURI"], table_name_with_prefix, field)
    else:
        key_tuple = []
        for field in fields:
            if field not in config["context"]["uniqueField"]:
                key_tuple.append(
                    [f"{field}-{table_name}", join_url(config["context"]["baseURI"], field, table_name_with_prefix)])

        if table_name == "location2externalid":
            key_tuple = [t for t in key_tuple if t[0] != 'relation-location2externalid']

        if len(key_tuple) > 1:
            context[key_tuple[0][0]] = key_tuple[1][1]
            context[key_tuple[1][0]] = key_tuple[0][1]

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


def search_json_for_value(data: Any, value: Any, path: str = "") -> str:
    if data == value:
        return path

    if isinstance(data, dict):
        for key in data:
            result = search_json_for_value(data[key], value, f"{path}.{key}" if path else key)
            if result:
                return result

    return ""


def get_object_from_path(data: Any, path: str) -> Any:
    keys = path.split(".")
    current = data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None

    return current


def convert_json_ld_to_ttl(json_ld: Dict) -> str:
    try:
        # Create an RDF graph
        graph = Graph()

        # Parse the JSON-LD into the graph
        graph.parse(data=json.dumps(json_ld), format="json-ld")

        # Serialize the graph to Turtle format
        return graph.serialize(format="turtle")
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


def save_json_ld_to_file(json_ld: Dict, file_path: str) -> None:
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

    if distance < 1:
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
                    is_outgoing = related_tables[table_name].get("outgoing", []) and key in related_tables[table_name][
                        "outgoing"]
                    record_data[f"{table_name_with_prefix}-{key}"] = (
                        {"@id": join_url(config["context"]["baseURI"], key, record[key])}
                        if is_outgoing else record[key]
                    )

        if "records" not in related_tables[table_name]:
            related_tables[table_name]["records"] = []

        related_tables[table_name]["records"].append(record_data["@id"])
        graph.append(record_data)
    else:
        key_tuple = []
        for key in record:
            if key not in config["context"]["uniqueField"]:
                try:
                    key_tuple.append([
                        f"{key}-{table_name}",
                        join_url(config["context"]["baseURI"], key, record[key])
                    ])
                except Exception as e:
                    logger.error(
                        f"Error adding record to graph: {e} {config['context']['baseURI']} {key} {json.dumps(record, indent=2)}")
                    raise

        if table_name == "location2externalid":
            key_tuple = [t for t in key_tuple if t[0] != 'relation-location2externalid']

        if len(key_tuple) >= 2:
            key1, key2 = key_tuple[0][0], key_tuple[1][0]
            value1, value2 = key_tuple[0][1], key_tuple[1][1]

            path1 = search_json_for_value(json_ld, value2)
            path2 = search_json_for_value(json_ld, value1)

            obj1 = get_object_from_path(json_ld, ".".join(path1.split(".")[:2])) if path1 else None
            obj2 = get_object_from_path(json_ld, ".".join(path2.split(".")[:2])) if path2 else None

            if obj1 and obj2:
                obj1[key1] = {"@id": value1}
                obj2[key2] = {"@id": value2}

    json_ld["@graph"] = graph
    return json_ld


def main():
    args = parse_args()
    table_name = args.tableName
    distance: int = args.distance

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
                json_ld = add_table_fields_to_context(json_ld, related_table_name, table["metadata"].get("fields", {}))

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
                json_ld = add_table_fields_to_context(json_ld, related_table_name, table["metadata"].get("fields", {}))

                for record in table["data"]:
                    json_ld = add_record_to_graph(json_ld, related_table_name, related_tables, record, "", False)

        # Process middle tables
        for related_table_name in related_tables:
            if related_table_name in config["context"]["middleTables"]:
                logger.info(f"Processing middle table: '{related_table_name}'")
                table = cache_related_tables[related_table_name]
                json_ld = add_table_fields_to_context(json_ld, related_table_name, table["metadata"].get("fields", {}))

                if related_table_name == "location2externalid":
                    logger.debug(f"Processing record from middle table: '{related_table_name}'")

                for record in table["data"]:
                    json_ld = add_record_to_graph(json_ld, related_table_name, related_tables, record)

        # Save results
        output_dir = config["outputDir"]
        os.makedirs(output_dir, exist_ok=True)

        output_json_path = os.path.join(output_dir, config["outputJsonLd"])
        save_json_ld_to_file(json_ld, output_json_path)

        try:
            turtle = convert_json_ld_to_ttl(json_ld)

            if validate_ttl(turtle):
                output_ttl_path = os.path.join(output_dir, config["outputRdf"])
                with open(output_ttl_path, "w", encoding="utf-8") as f:
                    f.write(turtle)
                logger.info(f"Turtle data saved to {output_ttl_path}")
            else:
                logger.error("TTL is not valid")
                return
        except Exception as e:
            logger.error(f"Error during conversion to TTL: {e}")
            return

        logger.info("Done")
    finally:
        http_client.close()


if __name__ == "__main__":
    main()
