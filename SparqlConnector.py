from SPARQLWrapper import SPARQLWrapper, XML, POST
from Constants import PREFIXES


class SparqlConnector:
    sparql: SPARQLWrapper

    def __init__(self, url_to_sparql_endpoint: str):
        self.sparql = SPARQLWrapper(url_to_sparql_endpoint)
        self.sparql.setReturnFormat(XML)
        self.sparql.setMethod(POST)
        self.sparql.setCredentials("demo", "demo")

    def delete_subject(self, subject: str):
        self.sparql.setQuery(f"""
            {PREFIXES}
            DELETE WHERE {{
                {subject} ?predicate ?object .
            }}
        """)
        print(self.sparql.queryString)
        # self.sparql.query()

    def insert_triples(self, triples: str):
        self.sparql.setQuery(f"""
            {PREFIXES}
            INSERT DATA {{
                {triples}
            }}
        """)
        print(self.sparql.queryString)
        # self.sparql.queryAndConvert()

    def modify_subject(self, subject: str, predicate: str, object: str):
        print()
