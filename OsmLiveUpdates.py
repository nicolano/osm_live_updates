import logging
from http.client import HTTPResponse
from urllib.request import urlopen
import gzip
import io
import re
from xml.etree import ElementTree

from Osm2RdfConnector import Osm2RdfConnector
from SparqlConnector import SparqlConnector
from Constants import OSM_REPLICATION_BASE_URL, STATE_FILE_EXTENSION, CHANGE_FILE_EXTENSION


class OsmLiveUpdates:
    osm2rdfConnector: Osm2RdfConnector
    sparqlConnector: SparqlConnector

    def __init__(self, osm2rdf_path: str, osm2rdf_image_name: str, sparql_endpoint: str):
        self.osm2rdfConnector = Osm2RdfConnector(osm2rdf_path, osm2rdf_image_name)
        self.sparqlConnector = SparqlConnector(sparql_endpoint)

    def fetch_change(self, from_sequence_number: int):
        logging.info(f"Starting fetch from sequence number {str(from_sequence_number)}")

        latest_sequence_number = self.fetch_latest_sequence_number()
        logging.info(f"Latest sequence number is {str(latest_sequence_number)} so there are {str(latest_sequence_number - from_sequence_number)} diffs to fetch")

        sequence_number = from_sequence_number + 1
        while sequence_number <= latest_sequence_number:
            if self.state_exists_for_sequence_number(sequence_number):
                data: bytes = self.fetch_diff_for_sequence_number(sequence_number)
                root: ElementTree = ElementTree.fromstring(data)

                child: ElementTree.Element
                for child in root:
                    if child.tag == 'delete':
                        self.handle_delete(child)
                    elif child.tag == 'insert':
                        self.handle_insert(child)
                    elif child.tag == 'modify':
                        self.handle_modify(child)

                    break
                break
            else:
                logging.error(f"State for Sequence number {str(sequence_number)} does not exist")
                break

            # sequence_number += 1

    def handle_delete(self, to_delete: ElementTree.Element):
        for element in to_delete:
            elements_rdf_triples = self.osm2rdfConnector.convert(ElementTree.tostring(element))
            print(elements_rdf_triples)
            # for triple in elements_rdf_triples:
                # self.sparqlConnector.delete_subject(sub)

            break

    def handle_insert(self, to_insert: ElementTree.Element):
        for element in to_insert:
            rdf_triples = self.osm2rdfConnector.convert(ElementTree.tostring(element))
            self.sparqlConnector.insert_triples(rdf_triples)

            break

    def handle_modify(self, to_modify: ElementTree.Element):
        for element in to_modify:
            print(ElementTree.tostring(element))
            elements_rdf_triples = self.osm2rdfConnector.convert(ElementTree.tostring(element))
            print(elements_rdf_triples)

        # for triple in self.osm2rdfConnector.convert(ElementTree.tostring(element)):
                # self.sparqlConnector.modify_subject(sub, pred, ob)

            break

    def fetch_diff_for_sequence_number(self, sequence_number: int) -> bytes:
        """
        Fetches the diff file for the given sequence number from the osm server and decompresses it.
        :param sequence_number: The sequence number of the diff to fetch
        :return: The decompressed diff
        """
        logging.debug(f"Fetching data for sequence number {str(sequence_number)}")
        sequence_number_formatted = self.format_sequence_number_for_url(sequence_number)
        url = f"{OSM_REPLICATION_BASE_URL}/minute/{sequence_number_formatted}.{CHANGE_FILE_EXTENSION}"
        with urlopen(url) as f:
            f: HTTPResponse = f
            with gzip.GzipFile(fileobj=io.BytesIO(f.read())) as decompressed:
                return decompressed.read()

    def state_exists_for_sequence_number(self, sequence_number: int) -> bool:
        """
        Checks if there exists a state file for the given sequence number on the osm server. This is needed because
        incomplete diffs may be present that do not have a state file.
        :param sequence_number: Sequence number of the diff
        :return: True if the state file exists, False otherwise
        """
        logging.debug(f"Check if state exists for sequence number: {str(sequence_number)}")
        sequence_number_formatted = self.format_sequence_number_for_url(sequence_number)
        url = f"{OSM_REPLICATION_BASE_URL}/minute/{sequence_number_formatted}.{STATE_FILE_EXTENSION}"
        try:
            with urlopen(url) as f:
                sequence_number_fetched = self.get_sequence_number_from_state_file(f.read().decode("utf-8"))
        except:
            return False

        return sequence_number_fetched == sequence_number

    @staticmethod
    def get_sequence_number_from_state_file(file: str) -> int:
        """
        Extracts the sequence number from the given state file. The state file contains the date of upload, the sequence
        number and the timestamp of the diff.
        :param file: State file
        :return: The sequence number of the diff
        """
        pattern = r'sequenceNumber=(\d+)'
        match = re.search(pattern, file)
        return int(match.group(1))

    def fetch_latest_sequence_number(self) -> int:
        """
        Fetches the sequence number of the latest diff from the osm server.
        :return: The sequence number of the latest diff
        """
        url = f"{OSM_REPLICATION_BASE_URL}/minute/state.txt"
        with urlopen(url) as f:
            # Define the regex pattern to match the sequenceNumber
            return self.get_sequence_number_from_state_file(f.read().decode("utf-8"))

    @staticmethod
    def format_sequence_number_for_url(sequence_number: int) -> str:
        """
        Formats the sequence number in a way that it matches the format of the osm server. For example, the state file
        for the sequence number 6177383 can be fetched with the following url:
        https://planet.openstreetmap.org/replication/minute/006/177/383.state.txt
        So the formatted sequence number would look like this:
        006/177/383

        :param sequence_number: The sequence number
        :return: The formatted sequence number
        """
        sequence_number: str = str(sequence_number)

        while len(sequence_number) < 9:
            sequence_number = "0" + sequence_number

        return "{}/{}/{}".format(sequence_number[:3], sequence_number[3:6], sequence_number[6:9])


def main() -> None:
    logging.getLogger().setLevel(logging.DEBUG)
    sparql_endpoint = "http://Nicolass-MBP.fritz.box:7200/repositories/osm-test"

    olu = OsmLiveUpdates("/Users/nicolasvontrott/Documents/Masterproject/osm2rdf/osm2rdf", "nicolano/osm2rdf", sparql_endpoint)
    olu.fetch_change(6177382)

    # o2c = Osm2RdfConnector("/Users/nicolasvontrott/Documents/Masterproject/osm2rdf/osm2rdf")
    # o2c.convert("".encode())

    # sc = SparqlConnector(sparql_endpoint)
    # sc.delete_subject("athlete:JosephJosyStoffel")


if __name__ == "__main__":
    main()
