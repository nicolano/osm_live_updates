OSM_REPLICATION_BASE_URL = "https://planet.openstreetmap.org/replication"
CHANGE_FILE_EXTENSION = "osc.gz"
STATE_FILE_EXTENSION = "state.txt"
TEMPORARY_TAG = "TEMPORARY"

# Osm2RdfConnector
OSM_2_RDF_INPUT_FILE_NAME = "tmp.osm"
OSM_2_RDF_OUTPUT_FILE_NAME = "tmp.osm.ttl.bz2"

PREFIXES = """
PREFIX ohmnode: <https://www.openhistoricalmap.org/node/> 
PREFIX osmrel: <https://www.openstreetmap.org/relation/> 
PREFIX osmnode: <https://www.openstreetmap.org/node/> 
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:> 
PREFIX osmway: <https://www.openstreetmap.org/way/> 
PREFIX osmmeta: <https://www.openstreetmap.org/meta/> 
PREFIX osm: <https://www.openstreetmap.org/> 
PREFIX osm2rdfmeta: <https://osm2rdf.cs.uni-freiburg.de/rdf/meta#> 
PREFIX ohmrel: <https://www.openhistoricalmap.org/relation/> 
PREFIX osm2rdfmember: <https://osm2rdf.cs.uni-freiburg.de/rdf/member#> 
PREFIX osm2rdfkey: <https://osm2rdf.cs.uni-freiburg.de/rdf/key#> 
PREFIX osm2rdfgeom: <https://osm2rdf.cs.uni-freiburg.de/rdf/geom#> 
PREFIX ohmway: <https://www.openhistoricalmap.org/way/> 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
PREFIX ohm: <https://www.openhistoricalmap.org/> 
PREFIX wd: <http://www.wikidata.org/entity/> 
PREFIX osm2rdf: <https://osm2rdf.cs.uni-freiburg.de/rdf#> 
PREFIX ogc: <http://www.opengis.net/rdf#> 
PREFIX geo: <http://www.opengis.net/ont/geosparql#> 
"""