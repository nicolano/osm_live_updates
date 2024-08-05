from SPARQLWrapper import SPARQLWrapper, XML, POST
from Constants import PREFIXES, SPARQL_OUTPUT_FILE_NAME
import urllib.error
from enum import Enum


class OutputFormat(Enum):
    FILE = 1
    SPARQL_ENDPOINT = 2


class SparqlConnector:
    sparql: SPARQLWrapper
    output_format: OutputFormat

    def __init__(self, url_to_sparql_endpoint: str, output_format: OutputFormat = OutputFormat.SPARQL_ENDPOINT):
        """
        Initializes a Sparql Connector, who creates sparql queries and sends them to SPARQL endpoint or writes them to
        a file, depending on the output format.

        :param url_to_sparql_endpoint: The Url of the sparql endpoint
        :param output_format: The output format of the sparql endpoint.
        """
        self.output_format = output_format

        if output_format == OutputFormat.FILE:
            self.__write_to_output_file(f"{PREFIXES}", "w")
            return

        # Set up connection to sparql endpoint
        self.sparql = SPARQLWrapper(url_to_sparql_endpoint)
        self.sparql.setReturnFormat(XML)
        self.sparql.setMethod(POST)
        self.sparql.setCredentials("demo", "demo")

        # Test connection to sparql endpoint
        try:
            self.sparql.setQuery("""
                    DELETE { ?s ?p ?o } WHERE {
                        rdf:TEST_TEST_TEST ?p ?o .
                    }
                    """
            )
            self.sparql.query()
        except urllib.error.URLError:
            raise SparqlException("Could not connect to SPARQL endpoint")

    @staticmethod
    def __write_to_output_file(text: str, mode: str = "a"):
        f = open(SPARQL_OUTPUT_FILE_NAME, mode)
        f.write(text)
        f.close()

    def delete_subject(self, subject: str) -> None:
        """
        Creates and executes a sparql query that deletes all triplets containing the given subject
        :param subject: The subject for which the triplets are to be deleted
        """
        query: str = f"DELETE {{ {subject} ?p ?o }} WHERE {{ {subject} ?p ?o . }};\n"

        if self.output_format == OutputFormat.FILE:
            self.__write_to_output_file(text=query)
            return

        self.sparql.setQuery(
f"""
{PREFIXES}
{query}
"""
        )
        self.sparql.queryType = "INSERT"
        self.sparql.query()

    def insert_triples(self, triples: str) -> None:
        """
        Creates and executes a sparql query that inserts the passed triples.
        :param triples: The triples to insert
        """
        triples_formatted = triples.replace("\n", " ")
        query: str = f"INSERT DATA {{ {triples_formatted} }};\n"

        if self.output_format == OutputFormat.FILE:
            self.__write_to_output_file(text=query)
            return

        self.sparql.setQuery(
f"""
{PREFIXES}
{query}
"""
        )
        self.sparql.queryType = "DELETE"
        self.sparql.query()


class SparqlException(Exception):
    pass
