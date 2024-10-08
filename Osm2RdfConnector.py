import logging
from subprocess import Popen
from Constants import OSM_2_RDF_INPUT_FILE_NAME, OSM_2_RDF_OUTPUT_FILE_NAME
from shlex import split as shlex_split
from bz2 import open as bz2_open
from subprocess import DEVNULL, STDOUT


class Osm2RdfConnector:
    osm2rdf_path: str
    image_name: str

    def __init__(self, osm2rdf_path: str, image_name: str) -> None:
        """
        Connection layer for the osm2rdf tool, that is used to convert osm data to RDF Turtle.
        (see https://github.com/ad-freiburg/osm2rdf)

        :param osm2rdf_path: The path to the folder where osm2rdf is located.
        :param image_name: The name of the docker image for osm2rdf
        """

        self.osm2rdf_path = osm2rdf_path
        self.image_name = image_name

    def __write_input_to_file(self, data: bytes) -> None:
        """
        Packs the passed data in a xml element and writes it to the input file in the osm2rdf tool's 'input' folder
        :param data: Data to be written to the input file.
        """
        logging.debug("Writing osm data to input file")
        with open(self.osm2rdf_path + f"/input/{OSM_2_RDF_INPUT_FILE_NAME}", 'w') as file:
            content = f'<osmChange version="0.6" generator="osmdbt-create-diff/0.6">\n{data.decode()}\n</osmChange>'
            file.write(content)
            file.close()

    def __read_output_from_file(self) -> str:
        """
        Reads the content of the osm2rdf tools 'output' folder and returns it as a string.
        :return: The content of the 'output' folder.
        """
        logging.debug("Reading rdf data from output file")
        with bz2_open(self.osm2rdf_path + f"/output/{OSM_2_RDF_OUTPUT_FILE_NAME}") as file:
            return file.read().decode()

    def convert(self, osm_data: bytes) -> str:
        """
        Converts the passed osm data to RDF Turtle and returns the generated tuples.
        :param osm_data: The osm data to convert
        :return: The generated tuples in RDF Turtle format
        """
        self.__write_input_to_file(osm_data)
        self.__run()

        output = self.__remove_headers(self.__read_output_from_file())
        if output == "":
            logging.warning(f"No output generated for input: {osm_data}")

        return output

    @staticmethod
    def __remove_headers(string: str) -> str:
        """
        Removes all lines from the given string that begin with an '@', matching the headers that begin with
        '@prefix'
        """
        lines = string.split('\n')
        filtered_lines = [line for line in lines if not line.startswith('@')]
        return '\n'.join(filtered_lines)

    def __run(self) -> None:
        """
        Runs the docker command to execute the osm2rdf tool.
        """
        logging.debug("Start Conversion")
        args = shlex_split(f'docker run --rm '
                           f'-v {self.osm2rdf_path}/input/:/input/ '
                           f'-v {self.osm2rdf_path}/output/:/output/ '
                           f'-v {self.osm2rdf_path}/scratch/:/scratch/ '
                           f'{self.image_name} '
                           f'/input/{OSM_2_RDF_INPUT_FILE_NAME} '
                           f'-o /output/{OSM_2_RDF_OUTPUT_FILE_NAME} '
                           f'-t /scratch/')
        p = Popen(args, cwd=self.osm2rdf_path, stdout=DEVNULL, stderr=STDOUT)
        result = p.wait()
        if result != 0:
            raise Exception(f'Failed to convert to RDF')