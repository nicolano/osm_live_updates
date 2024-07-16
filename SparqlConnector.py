from SPARQLWrapper import SPARQLWrapper, XML, POST
from Constants import PREFIXES


class SparqlConnector:
    sparql: SPARQLWrapper

    def __init__(self, url_to_sparql_endpoint: str):
        self.sparql = SPARQLWrapper(url_to_sparql_endpoint)
        self.sparql.setReturnFormat(XML)
        self.sparql.setMethod(POST)
        self.sparql.setCredentials("demo", "demo")

    def delete_subject(self, subject: str) -> None:
        """
        Creates and executes a sparql query that deletes all triplets containing given subject as well as the osm2rdf
        geometry object.
        :param subject: The subject to delete
        """
        self.sparql.setQuery(
f"""
{PREFIXES}
DELETE {{ ?s ?p ?o }} WHERE {{
    ?s ?p ?o .
    FILTER (?s = {subject} || ?s = {self.__formate_subject_for_osm2rdfgeom(subject)} )
}}
"""
        )
        print(self.sparql.queryString)
        # self.sparql.query()

    def insert_triples(self, triples: str) -> None:
        """
        Creates and executes a sparql query that inserts the passed triples.
        :param triples: The triples to insert
        """
        self.sparql.setQuery(
f"""
{PREFIXES}
INSERT DATA {{
    {triples}
}}
"""
        )
        print(self.sparql.queryString)
        # self.sparql.query()

    def modify_subject(self, subject: str, new_triplets: str) -> None:
        """
        Delete all triplets for the given subject and insert the new triplets.
        :param subject: The subject to modify
        :param new_triplets: The new triples to insert
        """
        self.delete_subject(subject)
        self.insert_triples(new_triplets)

    @staticmethod
    def __formate_subject_for_osm2rdfgeom(subject: str) -> str:
        """
        Formats the subject name for the 'osm2rdfgeom' property, for example:
        Subject 'osmnode:1642992563' is formatted to: 'osm_node_1642992563'
        Subject 'osmway:7738035' is formatted to: 'osm2rdf:way_7738035'
        Subject 'osmrel:2727671' is formatted to: 'osm2rdf:rel_2727671'

        :param subject:
        :return:
        """
        formatted_subject: str
        if 'node' in subject:
            subject.replace(":", "_").replace("osm", "osm_")
            formatted_subject = f"osm2rdfgeom:{subject}"
        elif 'way' in subject:
            subject.replace("osmway:", "way_")
            formatted_subject = f"osm2rdf:{subject}"
        else:
            subject.replace("osmrel:", "rel_")
            formatted_subject = f"osm2rdf:{subject}"

        return formatted_subject
