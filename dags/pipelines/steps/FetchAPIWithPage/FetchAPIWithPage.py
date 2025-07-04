import os
import json
import logging
import httpx
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF
from .config import config

# Logging setup
log_level = os.getenv("LOG_LEVEL", config.get("logLevel", "INFO")).upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)

table_name_map = {
    "ccode": "countrycode",
    "lifespan": "timespan",
    "part_of": "locationpartof",
}


def join_url(base_url, *paths):
    url = base_url.rstrip("/")
    for part in paths:
        if part:
            url += "/" + str(part).strip("/")
    return url


def fetch_json(url):
    logger.debug(f"Fetching: {url}")
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.json()


def get_all_endpoints():
    tables = fetch_json(config["api"]["baseURL"])
    for stop_table in config["context"]["stopTables"]:
        tables.pop(stop_table, None)
    return tables


def fetch_table_rows(table_name):
    data = []
    next_url = join_url(config["api"]["baseURL"], table_name.lower())
    while next_url:
        result = fetch_json(next_url)
        data.extend(result.get("results", []))
        next_url = result.get("links", {}).get("next")
    return data


def fetch_table_metadata(table_name):
    url = join_url(config["api"]["baseURL"], table_name.lower())
    params = {"page": 1, "page_size": 1}
    resp = httpx.get(url, params=params)
    resp.raise_for_status()
    return resp.json().get("metadata", {})


def fetch_table(table_name):
    metadata = fetch_table_metadata(table_name)
    data = fetch_table_rows(table_name)
    return {"metadata": metadata, "data": data, "linkedTable": []}


def save_jsonld_to_file(jsonld_obj, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(jsonld_obj, f, indent=2)


def convert_jsonld_to_ttl(jsonld_obj):
    g = Graph()
    BASE = Namespace(config["context"]["baseURI"] + "/")
    g.bind("ex", BASE)
    for item in jsonld_obj["@graph"]:
        subj = URIRef(item["@id"])
        g.add((subj, RDF.type, Literal(item["@type"])))
        for k, v in item.items():
            if k.startswith("@"):
                continue
            pred = BASE[k]
            if isinstance(v, dict) and "@id" in v:
                obj = URIRef(v["@id"])
            else:
                obj = Literal(v)
            g.add((subj, pred, obj))
    return g.serialize(format="turtle")


def replace_table_name(table_name):
    if table_name in table_name_map:
        return table_name_map[table_name]
    return table_name


def get_related_tables(table_name: str, tables: dict) -> dict:
    related_tables = {table_name: {"incoming": [], "outgoing": []}}

    # Add outgoing foreign keys
    outgoing = []
    table_metadata = fetch_table_metadata(table_name)
    for related_table in table_metadata["foreign_keys"]:
        outgoing.append(related_table)
    related_tables[table_name]["outgoing"] = outgoing

    # Add incoming foreign keys
    incoming = []
    for table in tables:
        table_metadata = fetch_table_metadata(table)
        try:
            if table_name in table_metadata["foreign_keys"]:
                incoming.append(table)
        except Exception as error:
            logger.error(f"Error fetching foreign keys for table {table}: {error}")
            logger.info(f"Error fetching foreign keys for table {table}: {error}")
            raise Exception(f"Error fetching foreign keys for table {table}: {error}")
    related_tables[table_name]["incoming"] = incoming

    return related_tables


def get_related_tables_with_distance(table_name: str, tables, distance: int, related_tables=None):
    if related_tables is None:
        related_tables = {}
    table_name = replace_table_name(table_name)
    if distance < 1:
        return related_tables

    current_related_tables = get_related_tables(table_name, tables)
    related_tables[table_name] = current_related_tables

    for related_talbe in current_related_tables[table_name]["incoming"] or current_related_tables[table_name][
        "outgoing"]:
        if not related_tables[related_talbe]:
            get_related_tables_with_distance(related_talbe, tables, distance - 1, related_tables)

    return related_tables


def main(table_name, distance=1):
    # os.makedirs(config["outputDir"], exist_ok=True)
    table_prefix = ""
    tables = get_all_endpoints()
    table_keys = list(tables.keys())
    related_tables = get_related_tables_with_distance(table_name, tables, distance)

    jsonld = {
        "@context": {
            "@vocab": config["context"]["baseURI"],
            "id": "@id",
            "type": "@type"
        },
        "@graph": []
    }
    table = fetch_table(table_name)
    fields = table["metadata"].get("fields", {})
    for record in table["data"]:
        record_id = record.get("id")
        record_data = {
            "@id": join_url(config["context"]["baseURI"], table_name, record_id),
            "@type": table_name
        }
        for k, v in record.items():
            if k not in config["context"]["uniqueField"]:
                record_data[f"{table_name}-{k}"] = v
        jsonld["@graph"].append(record_data)
    jsonld_path = os.path.join(config["outputDir"], config["outputJsonLd"])
    save_jsonld_to_file(jsonld, jsonld_path)
    ttl = convert_jsonld_to_ttl(jsonld)
    ttl_path = os.path.join(config["outputDir"], config["outputRdf"])
    with open(ttl_path, "w", encoding="utf-8") as f:
        f.write(ttl)
    logger.info("Done")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--tableName", "-t", required=True)
    parser.add_argument("--distance", "-d", type=int, default=1)
    args = parser.parse_args()
    main(args.tableName, args.distance)
