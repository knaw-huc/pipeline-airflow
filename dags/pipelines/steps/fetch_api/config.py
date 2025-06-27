config = {
    "log_level": "info",
    "log_file": "app.log",
    "output_dir": "output",
    "output_jsonld": "output.jsonld",
    "output_rdf": "output.ttl",
    "api": {
        "base_url": "http://localhost/api"
    },
    "context": {
        "base_url": "http://example.globalise.nl/temp",
        "unique_field": ["id", "type"],
        "stop_tables": ["logentry", "permission", "group", "user", "contenttype", "session", "postgisgeometrycolumns",
                        "postgisspatialrefsys"],
        "middle_tables": ["timespan2source", "polity2source", "politylabel2source", " rulership2source",
                          "rulershiplabel2source", "rulergender2source", "ruler2source", "rulerlabel2source",
                          "reign2source", "location2countrycode", "location2coordsource", "location2source",
                          "location2externalid", "location2type", "locationtype2source", "locationlabel2source",
                          "locationpartof2source", "shiplabel2source", "ship2externalid", "ship2type",
                          "ship2source", "event2source", "event2location", "translocation2externalid",
                          "translocation2source", "translocation2location"
                          ],
        "main_entry_tables": ["polity", "politylabel", "reign", "ruler", "rulership", "rulershiplabel", "rulerlabel",
                              "locationlabel", "shiplabel", "location", "event", "translocation"],
        "notSure": ["locationpartof"]
    },
}
