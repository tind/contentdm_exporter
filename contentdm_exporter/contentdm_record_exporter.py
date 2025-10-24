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

import json
import logging
import math
import requests
import urllib.parse
from defusedxml.lxml import tostring, fromstring
from lxml.builder import E
from pathlib import Path
from logger import setup_logger


# Settings
# You can use the following URL to find the server number: https://mycontentdmsite.com/digital/api/diagnostics
# or https://mycontentdmsite.com/utils/diagnostics.
CDM_SERVER_NUMBER = "17267"

CDM_WEBSITE_URL = "https://http://digitallib.oit.edu/"

# Local path to save file
REL_PATH = (
    "/Users/kennethhole/TIND Implementation Dropbox/OIT/migration/CONTENTdm_export/"
)

# Collection alias
# If set to empty, the script will get the full list of collections and loop through all of them.
# Only set this variable if you plan to export a single collection.
# Used mainly for testing purposes. Example: ALIAS = "p17218coll3"
ALIAS = ""

# Use the config below if you want to export particular records from different collections.
# The format is a list of tuples with collection name and record IDs.
# Example: [('p15999coll1', '8'), ('yellowstone', '2')]
# To use this method, comment out the function run_batch()
# and uncomment the function run_export_list_of_records().
# MANUAL_EXPORT_LIST = [
#     ('p15999coll3', '8'),
#     ('yellowstone', '5845'),
#     ('BYUPhotos', '687'),
#     ('GEA', '8131'),
#     ('Jackson', '3422'),
#     ('MStar', '6590'),
#     ('p15999coll20', '37296'),
#     ('p15999coll22', '6664'),
#     ('p15999coll24', '9632'),
#     ('WomansExp', '2399')
# ]

#  Don't change CHUNK_SIZE unless CONTENTdm is timing out.
CHUNK_SIZE = 100

# Don't change $start_at from 1 unless you are exporting a range of records. If you
# want to export a range, use the number of the first record in the range.
START_AT = 1

# The last record in subset, not the entire record set. Don't change LAST_REC from 0
# unless you are exporting a subset of records. If you want to export a range, use the
# number of records in the subset, e.g., if you want to export 200 records, use that value.
LAST_REC = 0

# Do we like to export the page metadata?
# Exporting the page metadata will increase the time to do the export.
EXPORT_PAGE_METADATA = True

# Do we like to export the page metadata in JSON?
# This will be in addition to exporting the page metadata in XML.
# EXPORT_PAGE_METADATA need to be set to 'True' to be able to export page metadata at all.
EXPORT_PAGE_METADATA_JSON = False

# Other variables used by the script.
# URL to the CONTENTdm web services API.
MAIN_URL = "https://server{}.contentdm.oclc.org/dmwebservices/index.php?q=".format(
    CDM_SERVER_NUMBER
)

# Path to the output folder where you'll find the final xml file
OUTPUT_FOLDER = REL_PATH + "output/"


# Create output folder if it does not exists.
output_path = Path(OUTPUT_FOLDER)
if not output_path.is_dir():
    output_path.mkdir()

cdm_export_collections_path = Path(output_path, "collections")
if not cdm_export_collections_path.is_dir():
    cdm_export_collections_path.mkdir()

dm_query_files_path = Path(output_path, "dm_query_files")
if not dm_query_files_path.is_dir():
    dm_query_files_path.mkdir()

compound_metadata_files_path = Path(output_path, "compound_metadata_files")
if not compound_metadata_files_path.is_dir():
    compound_metadata_files_path.mkdir()


rec_num = 0  # Record counter


def get_list_of_collection_aliases():
    """
    Query CONTENTdm and return a list of collecton aliases
    """

    # GET collections
    query_url_collections = "{main_url}dmGetCollectionList/json".format(
        main_url=MAIN_URL
    )

    # Query CONTENTdm and return records; if failure, log problem.
    try:
        res = requests.get(query_url_collections)
        items = res.json()
    except Exception as e:
        logger.warning(
            "The following API request failed: %s. Error: %s"
            % (query_url_collections, e)
        )
        items = []
        pass

    # Create a list of aliases. Remove first letter which is a slash.
    return sorted([collection.get("alias")[1:] for collection in items])


def query_contentdm(query_map):
    """
    Query CONTENTdm with the values in $query_map and return an array of records.
    """

    # Create a query map
    # We query for as little possible info at this point since we'll be doing another query
    # on each item later.

    # We only want parent-level items, not pages/children. It appears that changing 'suppress'
    # to 0, as documented, has no effect anyway.

    query_url = "{main_url}dmQuery/{alias}/{searchstrings}/{fields}/{sortby}/{maxrecs}/{start_at}/{docptr}/{suggest}/{facets}/{format}".format(
        main_url=MAIN_URL,
        alias=query_map["alias"],
        searchstrings=query_map["searchstrings"],
        fields=query_map["fields"],
        sortby=query_map["sortby"],
        maxrecs=query_map["maxrecs"],
        start_at=query_map["start_at"],
        supress=query_map["supress"],
        docptr=query_map["docptr"],
        suggest=query_map["suggest"],
        facets=query_map["facets"],
        format=query_map["format"],
    )

    # Query CONTENTdm and return records; if failure, log problem.
    try:
        req = requests.get(query_url)
        items = req.json()
    except Exception as e:
        logger.warning(
            "The following API request failed: %s. Error: %s" % (query_url, e)
        )
        items = []
        pass

    return items


def get_number_of_records_in_collection(query_map):
    # Perform a preliminary query to determine how many records are in the current collection,
    # and to determine the number of queries required to get all the records.
    prelim_results = query_contentdm(query_map)

    return prelim_results["pager"]["total"]


def get_collection_sources(alias):
    # If a customer is using another field that source for the collections,
    # this query needs to be updated.
    # query_url = '{main_url}dmGetCollectionFieldVocabulary/{alias}/source/0/1/json'.format(
    #     main_url=MAIN_URL,
    #     alias=alias)
    # The previous method did not work well. We will try using the source facet directly.
    query_url = (
        "{main_url}digital/api/facet/facetfield/source/collection/{alias}".format(
            main_url=CDM_WEBSITE_URL, alias=alias
        )
    )

    # Query CONTENTdm and return records; if failure, log problem.
    try:
        req = requests.get(query_url)
        facet_fields = req.json()
    except Exception as e:
        logger.warning(
            "The following API request failed: %s. Error: %s" % (query_url, e)
        )
        facet_fields = {}
        pass

    sources = facet_fields.get("facets", {}).get("source", [])

    return sources


def get_compound_object_info(alias, pointer, format="json"):
    """
    Gets the item's compound info. "code" contains '-2' if the item is not compound.
    The alias starts with a slash, which is stripped away to keep the query url as
    simple to read as possible.
    """
    if alias.startswith("/"):
        alias = alias[1:]

    if format == "json":
        query_url = (
            MAIN_URL + "dmGetCompoundObjectInfo/" + alias + "/" + pointer + "/json"
        )
        req = requests.get(query_url)
        compound_info = json.loads(req.content)
    elif format == "xml":
        query_url = (
            MAIN_URL + "dmGetCompoundObjectInfo/" + alias + "/" + pointer + "/xml"
        )
        req = requests.get(query_url)
        compound_info = req.text
    return compound_info


def process_compound_object(compound_info):
    """
    Processes each record in the browse results.
    """
    cpd = fromstring(compound_info.encode("utf-8"))

    has_type = False
    for elem in cpd:
        if elem.tag == "type":
            has_type = True

    if has_type:
        string_positon = compound_info.index("?>") + 2
        compound_info = compound_info[string_positon:]

        return compound_info

    return ""


def get_item_info(alias, item_number, format="xml"):
    """
    Get the item information. Item can be parent record metadata or file/page metadata.
    """
    if alias.startswith("/"):
        alias = alias[1:]
    if format == "xml":
        query_url = MAIN_URL + "dmGetItemInfo/" + alias + "/" + item_number + "/xml"
        req = requests.get(query_url)
        item = req.text
        string_positon = item.index("?>") + 2
        item = item[string_positon:]
    elif format == "json":
        query_url = MAIN_URL + "dmGetItemInfo/" + alias + "/" + item_number + "/json"
        req = requests.get(query_url)
        item = json.loads(req.content)
    return item


def get_file_level_id(elem):
    file_level_id = None
    for page_elem in elem:
        if page_elem.tag == "pageptr":
            file_level_id = page_elem.text

    if file_level_id is None:
        logger.warning(
            "Compound object has no file level ID (pageptr): %s" % (tostring(elem),)
        )

    return file_level_id


def get_file_metadata_xml(file_level_id, collection_alias):
    pagemetadata = E.pagemetadata()
    if file_level_id is None:
        return pagemetadata

    # Add file metadata inside the compound object.
    # Use file_level_id (pageptr)) as the key.
    file_level_info = get_item_info(collection_alias, file_level_id, format="xml")
    file_level_xml = fromstring(file_level_info)
    # Strip away empty fields.
    pagemetadata = E.pagemetadata()
    for field in file_level_xml:
        if field.text or len(field) > 0:
            pagemetadata.append(field)

    return pagemetadata


def get_file_metadata_json(file_level_id, collection_alias):
    file_metadata = {}
    if file_level_id is None:
        return file_metadata

    # Add the file metadata to a separate file (JSON).
    file_level_info_json = get_item_info(collection_alias, file_level_id, format="json")
    # Strip away empty values
    for key, val in file_level_info_json.items():
        if val:
            file_metadata[key] = val

    return file_metadata


def get_bib_record(collection_alias, cdm_recid):
    # Create a new xml record object.
    record = E.record()

    record_compound_file_metadata = {}

    # Append CONTENTdm record ID to new record object.
    cdmid = E.cdmid(cdm_recid)
    record.append(cdmid)

    # Append the contentDM collection_alias to the new record object.
    cdmalias = E.cdmalias(collection_alias)
    record.append(cdmalias)

    # Get bibliographic record metadata
    bib_info = get_item_info(collection_alias, cdm_recid, format="xml")

    # Append each field to the new record object.
    bib_xml = fromstring(bib_info)
    for field in bib_xml:
        record.append(field)

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
                    pagemetadata_xml = get_file_metadata_xml(
                        file_level_id, collection_alias
                    )
                    if len(pagemetadata_xml) > 0:
                        # Append the page metadata to the page element.
                        elem.append(pagemetadata_xml)

                    if EXPORT_PAGE_METADATA_JSON:
                        # Get the page/file_metadata in JSON and export to a separate file
                        pagemetadata_json = get_file_metadata_json(
                            file_level_id, collection_alias
                        )
                        if pagemetadata_json:
                            record_compound_file_metadata[file_level_id] = (
                                pagemetadata_json
                            )

                elif elem.tag == "node":
                    for sub_elem in elem:
                        if sub_elem.tag == "page":
                            # Get the file level id
                            file_level_id = get_file_level_id(sub_elem)
                            # Get the page/file metadata
                            pagemetadata_xml = get_file_metadata_xml(
                                file_level_id, collection_alias
                            )
                            if len(pagemetadata_xml) > 0:
                                # Append the page metadata to the page element.
                                sub_elem.append(pagemetadata_xml)

                            if EXPORT_PAGE_METADATA_JSON:
                                # Get the page/file_metadata in JSON and export to a separate file
                                pagemetadata_json = get_file_metadata_json(
                                    file_level_id, collection_alias
                                )
                                if pagemetadata_json:
                                    record_compound_file_metadata[file_level_id] = (
                                        pagemetadata_json
                                    )
                        if sub_elem.tag == "node":
                            for sub_sub_elem in sub_elem:
                                if sub_sub_elem.tag == "page":
                                    # Get the file level id
                                    file_level_id = get_file_level_id(sub_sub_elem)
                                    # Get the page/file metadata
                                    pagemetadata_xml = get_file_metadata_xml(
                                        file_level_id, collection_alias
                                    )
                                    if len(pagemetadata_xml) > 0:
                                        # Append the page metadata to the page element.
                                        sub_sub_elem.append(pagemetadata_xml)

                                    if EXPORT_PAGE_METADATA_JSON:
                                        # Get the page/file_metadata in JSON and export to a separate file
                                        pagemetadata_json = get_file_metadata_json(
                                            file_level_id, collection_alias
                                        )
                                        if pagemetadata_json:
                                            record_compound_file_metadata[
                                                file_level_id
                                            ] = pagemetadata_json
        # Append the compound object to the record
        record.append(compound_xml)

    return record, record_compound_file_metadata


def save_output_xml_to_file(collection, alias, source_nb, processed_chunks):

    collection_path = Path(cdm_export_collections_path, alias)
    if not collection_path.is_dir():
        collection_path.mkdir()

    local_file_name = Path(
        collection_path,
        "cdm_export_{}_{:03}_{:03}.xml".format(alias, source_nb, processed_chunks),
    )
    with open(str(local_file_name), "wb") as xmlfile:
        xmlfile.write(tostring(collection, pretty_print=True, encoding="utf-8"))


def run_export_list_of_records():

    # You can make sure it is the main record ID (not child compund ID) by
    # checking that such a query returns -1 in the parent key.
    # https://server15999.contentdm.oclc.org/dmwebservices/index.php?q=GetParent/p15999coll22/6664/json

    collection = E.collection()
    compound_file_metadata = {}  # Export page metadata in a JSON file.

    aliases = []

    for collection_alias, cdm_recid in MANUAL_EXPORT_LIST:
        aliases.append(collection_alias)

        print("Processing ", cdm_recid)

        # Create the bib record with the contentDM record structure.
        record, record_compound_file_metadata = get_bib_record(
            collection_alias, cdm_recid
        )

        collection.append(record)

        if record_compound_file_metadata:
            compound_file_metadata[cdm_recid] = record_compound_file_metadata

    save_output_xml_to_file(collection, "multiple", 1, 1)

    if EXPORT_PAGE_METADATA_JSON:
        with open(str(Path(OUTPUT_FOLDER, "compound_file_metadata.json")), "w") as f:
            f.write(json.dumps(compound_file_metadata))


def run_batch():
    global rec_num

    if ALIAS:
        collection_aliases = [ALIAS]
    else:
        # Get the list of collections by their aliases.
        collection_aliases = get_list_of_collection_aliases()
    # collection_aliases = ['p17218coll2',
    #                       'p17218coll3',
    #                       'p17218coll4',
    #                       'p17218coll5',
    #                       'p17218coll7',
    #                       'p17218coll8',
    #                       'p17218coll9']

    logger.info("The following collections were found: %s" % (collection_aliases,))

    for alias in collection_aliases:

        print("Processing the collection: %s" % (alias,))

        collection_cdm_recids = (
            []
        )  # Used to check for duplicate record export on a collection level.

        # Get the number of records in a collection and calculate the number of chunks.
        query_map = {
            "alias": alias,
            "searchstrings": "0",
            "fields": "dmcreated",
            "sortby": "dmcreated!dmrecord",
            "maxrecs": CHUNK_SIZE,
            "start_at": 1,
            "supress": 1,
            "docptr": 0,
            "suggest": 0,
            "facets": 0,
            "format": "json",
        }
        nb_records_in_collection = get_number_of_records_in_collection(query_map)

        logger.info(
            "Total number of records in collection: %s is %s"
            % (alias, nb_records_in_collection)
        )

        # Go to the next collection if there are no records.
        if not nb_records_in_collection:
            continue

        # The querydm API can not export paginate more than 10 000 records.
        # It will then start to export the same records multiple times!
        # To avoid this, we need to query some bibliographic metadata to split
        # the results to below 10 000 records.
        # We will first try to split by the sub-collection (source).
        if nb_records_in_collection > 10000:
            total_number_in_sources = 0
            source_infos = []
            # Frist, we get the list of sub collections
            sources = get_collection_sources(alias)

            # Second, for each source, we query to get the number of records per source.
            total_count = 0
            for source_facet in sources:
                source_title = source_facet.get("title")
                source_count = source_facet.get("count")
                total_count += source_count
                search_string = "{index}^{query_string}^exact^and".format(
                    index="source", query_string=urllib.parse.quote_plus(source_title)
                )

                query_map = {
                    "alias": alias,
                    "searchstrings": search_string,
                    "fields": "dmcreated",
                    "sortby": "dmcreated",  # Of some reason, it returned zero if this was dmcreated!dmrecord for p17218coll2. Only dmrecord works too!
                    "maxrecs": CHUNK_SIZE,
                    "start_at": 1,
                    "supress": 1,
                    "docptr": 0,
                    "suggest": 0,
                    "facets": 0,
                    "format": "json",
                }
                nb_records_in_source = get_number_of_records_in_collection(query_map)
                if nb_records_in_source > 0:
                    source_infos.append(
                        {
                            "source_title": source_title,
                            "source_count": nb_records_in_source,
                        }
                    )
                    total_number_in_sources += nb_records_in_source

                if source_count != nb_records_in_source:
                    logger.warning(
                        "The number of records queried on the collection %s and source %s does not match the number from the facet. %s vs. %s"
                        % (alias, source_title, nb_records_in_source, source_count)
                    )

            if nb_records_in_collection != total_count:
                logger.warning(
                    "The number of records the collection %s does not match the total count from source: %s vs. %s"
                    % (alias, nb_records_in_collection, total_count)
                )

            if nb_records_in_collection != total_number_in_sources:
                logger.warning(
                    "The number of records the collection %s does not match the total number of records in the sources. %s vs. %s"
                    % (alias, nb_records_in_collection, total_number_in_sources)
                )
        else:
            source_infos = [
                {"source_title": "", "source_count": nb_records_in_collection}
            ]

        for i, source_info in enumerate(source_infos):
            if source_info.get("source_title"):
                print("Processing the source: %s" % (source_info.get("source_title"),))

            # We add one chunk, then round down.
            num_chunks = source_info.get("source_count") / CHUNK_SIZE + 1
            num_chunks = math.floor(num_chunks)

            compound_file_metadata = {}  # Export page metadata in a JSON file.

            # We need to restart the start_at number for each collection and each source value.
            start_at = START_AT
            processed_chunks = 1
            while processed_chunks <= num_chunks:
                # For each chunk, create a new collection xml object.
                collection = E.collection()
                print("Start at: ", start_at)

                if source_info.get("source_title"):
                    search_string = "{index}^{query_string}^exact^and".format(
                        index="source",
                        query_string=urllib.parse.quote_plus(
                            source_info.get("source_title")
                        ),
                    )
                else:
                    search_string = "0"

                query_map = {
                    "alias": alias,
                    "searchstrings": search_string,
                    "fields": "dmcreated",
                    "sortby": "dmcreated",  # Of some reason, it returned zero if this was dmcreated!dmrecord for p17218coll2. Only dmrecord works too!
                    "maxrecs": CHUNK_SIZE,
                    "start_at": start_at,
                    "supress": 1,
                    "docptr": 0,
                    "suggest": 0,
                    "facets": 0,
                    "format": "json",
                }

                # Query CONTENTdm for all records in a collection for the defined chunk.
                results = query_contentdm(query_map)
                if not results:
                    logger.warning(
                        "No records was found. Could not connect to CONTENTdm to start retrieving chunk starting at: %s"
                        % (start_at,)
                    )
                    continue

                # We have had an issue where the export contains duplicates.
                # It is because the pagination API is not stable enough, so that it has sometimes got fewer records in the result when paginating.
                # This results in the same records being exported twice.
                # For debugging purposes, we will add the query URL to the results export.
                query_url = "{main_url}dmQuery/{alias}/{searchstrings}/{fields}/{sortby}/{maxrecs}/{start_at}/{docptr}/{suggest}/{facets}/{format}".format(
                    main_url=MAIN_URL,
                    alias=query_map["alias"],
                    searchstrings=query_map["searchstrings"],
                    fields=query_map["fields"],
                    sortby=query_map["sortby"],
                    maxrecs=query_map["maxrecs"],
                    start_at=query_map["start_at"],
                    supress=query_map["supress"],
                    docptr=query_map["docptr"],
                    suggest=query_map["suggest"],
                    facets=query_map["facets"],
                    format=query_map["format"],
                )

                results["pager"]["query_url"] = query_url

                # Save the dm_query records so that we can analyze it.
                with open(
                    str(
                        Path(
                            dm_query_files_path,
                            "dm_query_{}_{:03}_{:03}.json".format(
                                alias, i + 1, processed_chunks
                            ),
                        )
                    ),
                    "w",
                ) as f:
                    f.write(json.dumps(results))

                # We are preparing the "start_at" number we will use in the next chunk.
                start_at = CHUNK_SIZE * processed_chunks + START_AT

                # Loop through each record in the processed chunk.
                for results_record in results["records"]:
                    rec_num += 1
                    print(rec_num)

                    if results_record["parentobject"] != -1:
                        logger.warning(
                            "The parentobject for cdmid %s is not -1: %s"
                            % (cdm_recid, results_record["parentobject"])
                        )

                    collection_alias = results_record["collection"]
                    if collection_alias.startswith("/"):
                        collection_alias = collection_alias[1:]
                    cdm_recid = str(results_record["pointer"])

                    # After introducing querying sub_collections/sources,
                    # we want to make sure that a record is not exported multiple times.
                    if cdm_recid in collection_cdm_recids:
                        logger.warning(
                            "The record %s is expported in %s. Duplicate!"
                            % (cdm_recid, alias)
                        )
                    collection_cdm_recids.append(cdm_recid)

                    # Create the bib record with the contentDM record structure.
                    record, record_compound_file_metadata = get_bib_record(
                        collection_alias, cdm_recid
                    )

                    # Save the compound file metadata in a separate JSON file.
                    if record_compound_file_metadata:
                        compound_file_metadata[cdm_recid] = (
                            record_compound_file_metadata
                        )

                    # # Append the record to the collection
                    collection.append(record)
                    if LAST_REC != 0:
                        if rec_num == LAST_REC:
                            save_output_xml_to_file(
                                collection, alias, i + 1, processed_chunks
                            )

                            # To get out of the while loop, make
                            # processed_chunks higher than num_chunks.
                            processed_chunks = num_chunks + 1
                            break

                save_output_xml_to_file(collection, alias, i + 1, processed_chunks)

                processed_chunks += 1

        if EXPORT_PAGE_METADATA_JSON:
            with open(
                str(
                    Path(
                        compound_metadata_files_path,
                        "compound_file_metadata_{}.json".format(alias),
                    )
                ),
                "w",
            ) as f:
                f.write(json.dumps(compound_file_metadata))


if __name__ == "__main__":
    # Create output folder if it does not exists.
    logger = setup_logger(
        str(Path(OUTPUT_FOLDER, "record_export.log")), "record_export"
    )
    logger = logging.getLogger("record_export")
    run_batch()

    # run_export_list_of_records()
