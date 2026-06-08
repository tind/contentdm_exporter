# Copyright (C) 2021 TIND.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
This script query a CONTENTdm collection for all bibliographic records (parent records).
It then loop through the records and query CONTENTdm for record metadata,
compound object metadata and (optional) bibliographic metadata stored on the file/page-level.
"""

import logging
import os
from pathlib import Path

# from defusedxml.lxml import tostring, fromstring
from lxml.etree import fromstring
from logger import setup_logger

from contentdm_record_exporter import (
    get_compound_object_info,
    process_compound_object,
    get_item_info,
    get_file_level_id,
)

from contentdm_file_exporter import get_all_records_from_file

from settings import CDM_SERVER_NUMBER, REL_PATH, EXPORT_PAGE_METADATA

# Other variables used by the script.
# URL to the CONTENTdm web services API.
MAIN_URL = (
    f"https://server{CDM_SERVER_NUMBER}.contentdm.oclc.org/dmwebservices/index.php?q="
)

# Path to the output folder where you'll find the final xml file
OUTPUT_FOLDER = REL_PATH + "output/fulltext/"

# Path to the folder where you'll find the input xml file(s) downloaded with
# contentdm_record_exporter.
INPUT_FOLDER = REL_PATH + "output/collections/"


def get_export_list(input_folder):
    """
    Read through XML metadata exported via record export step and identify
    records that contain populated fulltext fields. Return list of collection
    aliases and CDM record IDs to pull full text for.
    """
    export_list = []
    input_path = Path(input_folder)
    for file_path in sorted(input_path.glob("**/*.xml")):
        print("Processing the file: ", file_path)
        collection = get_all_records_from_file(file_path)
        for record in collection:
            # check if has fulltext field
            if record.xpath("full/text()"):
                dmrecord = record.xpath("cdmid")[0].text
                alias = record.xpath("cdmalias")[0].text
                export_list.append((alias, dmrecord))
    return export_list


def get_fulltext_from_cdm(collection_alias, cdm_recid):
    item_info = get_item_info(collection_alias, cdm_recid, format="xml")
    try:
        item_xml = fromstring(item_info)
    except Exception:
        item_xml = fromstring(item_info.encode("utf-8"))

    fulltext_values = item_xml.xpath("child::full/text()")
    fulltext_record = {"cdmid": cdm_recid, "full": fulltext_values}
    return fulltext_record


def get_file_fulltext_record(file_level_id, collection_alias):
    if file_level_id is None:
        return {}
    return get_fulltext_from_cdm(collection_alias, file_level_id)


def get_fulltext_record(collection_alias, cdm_recid):
    fulltext_records = []

    parent_fulltext_record = get_fulltext_from_cdm(collection_alias, cdm_recid)
    fulltext_records.append(parent_fulltext_record)

    # Get the records compound information.
    compound_info = get_compound_object_info(collection_alias, cdm_recid, "xml")
    compound_info = process_compound_object(compound_info)
    if compound_info:
        compound_xml = fromstring(compound_info)
        compound_xml.tag = "structure"
        # Compound objects can contain metadata for each page.
        # Get the page metadata and store it inside the page object and as
        # a separate file (JSON).
        if EXPORT_PAGE_METADATA:
            # Loop through the compound object and find each page.
            # Append the page metadata to the page element.
            # A page can be found on three levels in the XML object.
            # - page
            # - node
            #   - page
            # - node
            #   - node
            #     - page
            for elem in compound_xml:
                if elem.tag == "page":
                    # Get the file level id
                    file_level_id = get_file_level_id(elem)
                    # Get the page/file metadata
                    file_level_fulltext = get_file_fulltext_record(
                        file_level_id, collection_alias
                    )
                    if file_level_fulltext:
                        fulltext_records.append(file_level_fulltext)

                elif elem.tag == "node":
                    for sub_elem in elem:
                        if sub_elem.tag == "page":
                            # Get the file level id
                            file_level_id = get_file_level_id(sub_elem)
                            file_level_fulltext = get_file_fulltext_record(
                                file_level_id, collection_alias
                            )
                            if file_level_fulltext:
                                fulltext_records.append(file_level_fulltext)

                        if sub_elem.tag == "node":
                            for sub_sub_elem in sub_elem:
                                if sub_sub_elem.tag == "page":
                                    # Get the file level id
                                    file_level_id = get_file_level_id(sub_sub_elem)
                                    file_level_fulltext = get_file_fulltext_record(
                                        file_level_id, collection_alias
                                    )
                                    if file_level_fulltext:
                                        fulltext_records.append(file_level_fulltext)
    return fulltext_records


def run_fulltext_export_list_of_records(export_list, fulltext_output_path):

    for collection_alias, cdm_recid in export_list:
        print("Processing ", cdm_recid)
        coll_output_path = Path(fulltext_output_path, collection_alias)
        if not coll_output_path.is_dir():
            coll_output_path.mkdir()

        rec_output_path = Path(coll_output_path, cdm_recid)
        if not rec_output_path.is_dir():
            rec_output_path.mkdir()

        # Create the bib record with the contentDM record structure.
        fulltext_records = get_fulltext_record(collection_alias, cdm_recid)

        for record in fulltext_records:
            record_cdmid = record["cdmid"]
            child_output_path = Path(rec_output_path, record_cdmid)
            if not child_output_path.is_dir():
                child_output_path.mkdir()
            with open(
                os.path.join(
                    child_output_path, f"{collection_alias}_{record_cdmid}.txt"
                ),
                "w",
                encoding="utf-8",
            ) as f:
                for val in record["full"]:
                    f.write(val)


if __name__ == "__main__":
    # Create output folder if it does not exists.
    setup_logger(str(Path(OUTPUT_FOLDER, "record_export.log")), "record_export")
    logger = logging.getLogger("record_export")

    # Create output folder if it does not exists.
    output_path = Path(OUTPUT_FOLDER)
    if not output_path.is_dir():
        output_path.mkdir()

    fulltext_export_list = get_export_list(INPUT_FOLDER)
    run_fulltext_export_list_of_records(fulltext_export_list, output_path)
