import logging
from http.client import HTTPResponse
from urllib.request import urlopen
import gzip
import io
import re
from xml.etree import ElementTree

from Osm2RdfConnector import Osm2RdfConnector
from SparqlConnector import SparqlConnector
from Constants import OSM_REPLICATION_BASE_URL, STATE_FILE_EXTENSION, CHANGE_FILE_EXTENSION, TEMPORARY_TAG


class OsmLiveUpdates:
    osm2rdfConnector: Osm2RdfConnector
    sparqlConnector: SparqlConnector

    def __init__(self, osm2rdf_path: str, osm2rdf_image_name: str, sparql_endpoint: str):
        self.osm2rdfConnector = Osm2RdfConnector(osm2rdf_path, osm2rdf_image_name)
        self.sparqlConnector = SparqlConnector(sparql_endpoint)

    def fetch_change(self, from_sequence_number: int):
        logging.info(f"Starting fetch from sequence number {str(from_sequence_number)}")

        latest_sequence_number = self.__fetch_latest_sequence_number()
        logging.info(f"Latest sequence number is {str(latest_sequence_number)} so there are {str(latest_sequence_number - from_sequence_number)} diffs to fetch")

        sequence_number = from_sequence_number + 1
        while sequence_number <= latest_sequence_number:
            if self.__state_exists_for_sequence_number(sequence_number):
                data: bytes = self.__fetch_diff_for_sequence_number(sequence_number)
                root: ElementTree = ElementTree.fromstring(data)

                child: ElementTree.Element
                for child in root:
                    if child.tag == 'delete':
                        for element in child:
                            self.__handle_delete(element)
                    elif child.tag == 'insert':
                        for element in child:
                            self.__handle_insert(element)
                    elif child.tag == 'modify':
                        for element in child:
                            self.__handle_modify(element)
            else:
                logging.error(f"State for Sequence number {str(sequence_number)} does not exist")

            # sequence_number += 1

    def __handle_delete(self, element: ElementTree.Element) -> None:
        """
        Handles element that is marked as to delete.
        :param element: Element to delete.
        """
        identifier = element.attrib['id']
        element_name = self.__get_element_name(element)

        # Delete all triplets containing the subject
        subject = f"osm{element_name}:{identifier}"
        self.sparqlConnector.delete_subject(subject)

        # Delete the triplet that contains the osm2rdf geo object
        formatted_subject = self.__formate_subject_for_osm2rdfgeom(subject)
        self.sparqlConnector.delete_subject(formatted_subject)

    def __handle_insert(self, element: ElementTree.Element):
        """
        Handles element that is marked to be inserted.
        :param element: Element to insert.
        """
        # Add a tag temporary tag to the element, otherwise osm2rdf will ignore the element.
        element_needs_temporary_tag: bool
        try:
            element_needs_temporary_tag = len(element.getchildren()) == 0
        except AttributeError:
            element_needs_temporary_tag = True

        if element_needs_temporary_tag:
            self.__add_temporary_tag(element)

        # Convert the osm data to the rdf format
        element_string = ElementTree.tostring(element).rstrip()
        rdf_triples = self.osm2rdfConnector.convert(element_string)

        if element_needs_temporary_tag:
            rdf_triples = self.__remove_triplets_for_temporary_tag(rdf_triples)

    # Insert the triplets to the database
        self.sparqlConnector.insert_triples(rdf_triples)

    def __handle_modify(self, element: ElementTree.Element):
        """
        Handles all element that is marked to be modified, which means deleting the old triplets and inserting the
        new ones.
        :param element: Element to be modified.
        """
        self.__handle_delete(element)
        self.__handle_insert(element)

    @staticmethod
    def __add_temporary_tag(element: ElementTree.Element) -> None:
        """
        Adds a sub-element with tag 'tag' and key 'k' and value 'v' 'TEMPORARY' to the passed element. This is needed
        because osm2rdf doesn't convert elements without a tag. The resulting sub-element looks like this:
              <tag k="TEMPORARY" v="TEMPORARY"/>
        :param element: The element to which the sub-element is to be added
        """
        child = ElementTree.SubElement(element, "tag")
        child.set("k", TEMPORARY_TAG)
        child.set("v", TEMPORARY_TAG)

    @staticmethod
    def __remove_triplets_for_temporary_tag(triplets: str) -> str:
        """
        Removes the triplets that were created for the temporary tag.
        :param triplets: The triplets to be cleared from temporary one
        :return: The triplets without the triplet for the temporary one
        """
        return re.sub(f".*{TEMPORARY_TAG}.*\n?","", triplets)

    def __fetch_diff_for_sequence_number(self, sequence_number: int) -> bytes:
        """
        Fetches the diff file for the given sequence number from the osm server and decompresses it.
        :param sequence_number: The sequence number of the diff to fetch
        :return: The decompressed diff
        """
        logging.debug(f"Fetching data for sequence number {str(sequence_number)}")
        sequence_number_formatted = self.__format_sequence_number_for_url(sequence_number)
        url = f"{OSM_REPLICATION_BASE_URL}/minute/{sequence_number_formatted}.{CHANGE_FILE_EXTENSION}"
        with urlopen(url) as f:
            f: HTTPResponse = f
            with gzip.GzipFile(fileobj=io.BytesIO(f.read())) as decompressed:
                return decompressed.read()

    def __state_exists_for_sequence_number(self, sequence_number: int) -> bool:
        """
        Checks if there exists a state file for the given sequence number on the osm server. This is needed because
        incomplete diffs may be present that do not have a state file.
        :param sequence_number: Sequence number of the diff
        :return: True if the state file exists, False otherwise
        """
        logging.debug(f"Check if state exists for sequence number: {str(sequence_number)}")
        sequence_number_formatted = self.__format_sequence_number_for_url(sequence_number)
        url = f"{OSM_REPLICATION_BASE_URL}/minute/{sequence_number_formatted}.{STATE_FILE_EXTENSION}"
        try:
            with urlopen(url) as f:
                sequence_number_fetched = self.__get_sequence_number_from_state_file(f.read().decode("utf-8"))
        except:
            return False

        return sequence_number_fetched == sequence_number

    @staticmethod
    def __get_sequence_number_from_state_file(file: str) -> int:
        """
        Extracts the sequence number from the given state file. The state file contains the date of upload, the sequence
        number and the timestamp of the diff.
        :param file: State file
        :return: The sequence number of the diff
        """
        pattern = r'sequenceNumber=(\d+)'
        match = re.search(pattern, file)
        return int(match.group(1))

    def __fetch_latest_sequence_number(self) -> int:
        """
        Fetches the sequence number of the latest diff from the osm server.
        :return: The sequence number of the latest diff
        """
        url = f"{OSM_REPLICATION_BASE_URL}/minute/state.txt"
        with urlopen(url) as f:
            # Define the regex pattern to match the sequenceNumber
            return self.__get_sequence_number_from_state_file(f.read().decode("utf-8"))

    @staticmethod
    def __format_sequence_number_for_url(sequence_number: int) -> str:
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

    @staticmethod
    def __get_element_name(element: ElementTree.Element) -> str:
        """
        Returns the name of the element, which is its tag for nodes and ways and 'rel' for relations.
        :param element:
        :return:
        """
        element_name: str
        if element.tag == 'relation':
            element_name = 'rel'
        else:
            element_name = element.tag

        return element_name

def main() -> None:
    logging.getLogger().setLevel(logging.DEBUG)
    sparql_endpoint = "http://Nicolass-MBP.fritz.box:7200/repositories/osm-test/statements"

    olu = OsmLiveUpdates("/Users/nicolasvontrott/Documents/Masterproject/osm2rdf/osm2rdf", "nicolano/osm2rdf", sparql_endpoint)
    olu.fetch_change(6176377)

    # o2c = Osm2RdfConnector("/Users/nicolasvontrott/Documents/Masterproject/osm2rdf/osm2rdf")
    # o2c.convert("".encode())

    # sc = SparqlConnector(sparql_endpoint)
    # sc.delete_subject("athlete:JosephJosyStoffel")


if __name__ == "__main__":
    main()
