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
        Creates and executes a sparql query that deletes all triplets containing the given subject
        :param subject: The subject for which the triplets are to be deleted
        """
        self.sparql.setQuery(
f"""
{PREFIXES}
DELETE {{ ?s ?p ?o }} WHERE {{
    {subject} ?p ?o .
}}
"""
        )
        self.sparql.queryType = "INSERT"
        self.sparql.query()

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
        self.sparql.queryType = "DELETE"
        self.sparql.query()
