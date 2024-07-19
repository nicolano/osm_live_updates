from typing import Dict

import numpy as np
import numpy.typing as npt

from OsmLiveUpdates import OsmLiveUpdates
import logging
from xml.etree import ElementTree


class Statistics:
    olu: OsmLiveUpdates

    def __init__(self, osm2rdf_path: str, osm2rdf_image_name: str, sparql_endpoint: str):
        self.olu = OsmLiveUpdates(osm2rdf_path, osm2rdf_image_name, sparql_endpoint)

    def mean_number_of_changes_per_diff(self, number_of_diffs_to_check: int = 10):
        changes_per_diff = {}

        latest_sequence_id = self.olu.fetch_latest_sequence_number() - 1000
        for i in range(number_of_diffs_to_check):
            # number_of_nodes_to_delete: index = 0
            # number_of_nodes_to_insert: index = 1
            # number_of_nodes_to_modify: index = 2
            #
            # number_of_ways_to_delete: index = 3
            # number_of_ways_to_insert: index = 4
            # number_of_ways_to_modify: index = 5
            #
            # number_of_relations_to_delete: index = 6
            # number_of_relations_to_insert: index = 7
            # number_of_relations_to_modify: index = 8
            counters: np.ndarray = np.zeros(9)

            seq_number = latest_sequence_id - i
            num_of_sequences_to_analyze = number_of_diffs_to_check - (latest_sequence_id - seq_number)
            logging.info(f"Analyze diff with sequence number {seq_number}, {num_of_sequences_to_analyze} more to go")
            diff_data: bytes = self.olu.fetch_diff_for_sequence_number(seq_number)
            root: ElementTree = ElementTree.fromstring(diff_data)
            for element in root:
                if element.tag == 'delete':
                    for child_element in element:
                        if child_element.tag == "way":
                            counters[3] += 1
                        elif child_element.tag == "relation":
                            counters[6] += 1
                        elif child_element.tag == "node":
                            counters[0] += 1
                elif element.tag == 'create':
                    for child_element in element:
                        if child_element.tag == "way":
                            counters[4] += 1
                        elif child_element.tag == "relation":
                            counters[7] += 1
                        elif child_element.tag == "node":
                            counters[1] += 1
                elif element.tag == 'modify':
                    for child_element in element:
                        if child_element.tag == "way":
                            counters[5] += 1
                        elif child_element.tag == "relation":
                            counters[8] += 1
                        elif child_element.tag == "node":
                            counters[2] += 1

            changes_per_diff[seq_number] = counters

        print(
            f"""
{number_of_diffs_to_check} diffs where analyzed.

A diff contained in mean {self.get_mean_number_of_changes(changes_per_diff)} changes.

{self.get_mean_number_of(changes_per_diff, 1, 4, 7)} of this changes where insertion of elements
{self.get_mean_number_of(changes_per_diff, 0, 3, 6)} of this changes where deletion of elements
{self.get_mean_number_of(changes_per_diff, 2, 5, 8)} of this changes where modifying of elements

{self.get_mean_number_of(changes_per_diff, 0, 1, 2)} of this changes where for nodes
{self.get_mean_number_of(changes_per_diff, 3, 4, 5)} of this changes where for ways
{self.get_mean_number_of(changes_per_diff, 6, 7, 8)} of this changes where relations
            """
        )

    @staticmethod
    def get_mean_number_of_changes(counters) -> float:
        total_number_of_changes: int = 0
        for seq_num in counters:
            total_number_of_changes += np.sum(counters[seq_num])

        return total_number_of_changes / len(counters)

    @staticmethod
    def get_mean_number_of(counters, idx1: int, idx2: int, idx3: int) -> float:
        total_number_of_insertions: int = 0
        for seq_num in counters:
            total_number_of_insertions += counters[seq_num][idx1]
            total_number_of_insertions += counters[seq_num][idx2]
            total_number_of_insertions += counters[seq_num][idx3]

        return total_number_of_insertions / len(counters)


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    sparql_endpoint = "http://Nicolass-MBP.fritz.box:7200/repositories/osm-test/statements"
    osm2rdf_path = "/Users/nicolasvontrott/Documents/Masterproject/osm2rdf/osm2rdf"
    osm2rdf_image_name = "nicolano/osm2rdf"

    statistics = Statistics(osm2rdf_path, osm2rdf_image_name, sparql_endpoint)
    statistics.mean_number_of_changes_per_diff(1000)


if __name__ == '__main__':
    main()
