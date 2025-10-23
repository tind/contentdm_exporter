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
This script loops through the input XML and find all files. It then query CONTENTdm and download
the files.
"""

import logging
import multiprocessing
import requests
import shutil
from defusedxml.lxml import parse
from pathlib import Path

from logger import setup_logger

# Settings
# You can use the following URL to find the server number: https://mycontentdmsite.com/digital/api/diagnostics
# or https://mycontentdmsite.com/utils/diagnostics.
CDM_SERVER_NUMBER = "16923"

CDM_WEBSITE_URL = "https://societyofthecincinnati.contentdm.oclc.org/"

# The local path
REL_PATH = "/Users/Demo/migration/my_project/"

FILE_URL = CDM_WEBSITE_URL + "utils/getfile/collection/"
# The file URL can also be found on the format:
# FILE_URL = 'https://cdm{}contentdm.oclc.org/utils/getfile/collection/'.format(CDM_SERVER_NUMBER)

# Path to the folder where you'll find the input xml file(s) that was downloaded with contentdm_record_exporter.
INPUT_FOLDER = REL_PATH + "collections/"

# Path to the output folder where the downloaded files will be stored.
OUTPUT_FOLDER = REL_PATH + "Download/"
# OUTPUT_FOLDER = "/home/upload/data/UTSW/Downloads/"


def get_all_records_from_file(file_path):
    xml = parse(str(file_path))
    collection = xml.getroot()
    return collection


def download_file(alias, page_id, output_path, filename):
    """
    Export file from CONTENTdm
    filename is the parameter in the CONTENTdm query which defines the local file
    name. It has nothing to do with what the name is on the server.
    """
    local_file_name = Path(output_path, filename)

    # If the file already exists, skip downloading it again.
    if not local_file_name.is_file():
        download_url = FILE_URL + alias + "/id/" + page_id + "/filename/" + filename
        # Download file
        print(download_url)
        try:
            # Get result from url and write to file:
            file_response = requests.get(download_url, stream=True)
            if file_response.status_code == 200:
                with open(local_file_name, "wb") as output_file:
                    shutil.copyfileobj(file_response.raw, output_file)
                return True
            else:
                logger.warning(
                    "Download failed for the URL: %s. Status Code: %s. Message: %s"
                    % (download_url, file_response.status_code, file_response.text)
                )
            # req = requests.get(download_url, timeout=3600)
            # if len(req.content) < 1000:
            #     if req.text == 'Requested item not found':
            #         print('File does not exists. Record: ', page_id)
            #         return 'Requested item not found'
            # if req.status_code == 200:
            #     with open(str(local_file_name), 'wb') as f:
            #         f.write(req.content)
            #     return True
            # else:
            #     return False
        except requests.exceptions.Timeout as e:
            logger.warning(
                "Download failed for the URL: %s. Error: %s" % (download_url, e)
            )
            return e
    else:
        # TIND specific usage as we import the function from another script.
        return "local"


def get_page_info(elem):
    has_pdfpage = False
    page_id = ""
    pathinfo = None
    for sub_elem in elem:
        if sub_elem.tag == "pagefile":
            if sub_elem.text.endswith(".pdfpage"):
                has_pdfpage = True
            pathinfo = Path(sub_elem.text)
        elif sub_elem.tag == "pageptr":
            page_id = sub_elem.text
    return has_pdfpage, page_id, pathinfo


if __name__ == "__main__":
    logger = setup_logger(str(Path(OUTPUT_FOLDER, "file_export.log")), "file_export")
    logger = logging.getLogger("file_export")
    # Loop through all files in path, except DS_Store (MacOS specific files).
    input_path = Path(INPUT_FOLDER)
    for file_path in sorted(input_path.glob("**/*.xml")):
        print("Processing the file: ", file_path)
        collection = get_all_records_from_file(file_path)
        # Loop through records
        files_to_download = []
        for record in collection:
            all_files_in_record = []
            # Decide about local path to download files
            dmrecord = record.xpath("dmrecord")[0].text  # We could also have used cdmid

            alias = record.xpath("cdmalias")[0].text

            output_path = Path(OUTPUT_FOLDER)
            if not output_path.is_dir():
                output_path.mkdir()

            output_path = Path(OUTPUT_FOLDER, alias)
            if not output_path.is_dir():
                output_path.mkdir()
            output_path = Path(output_path, dmrecord)
            if not output_path.is_dir():
                output_path.mkdir()

            # Check if the record has an element "structure" with children
            structures = record.xpath("structure")
            if structures and len(structures[0]) > 0:
                download_pdf = False
                # First, let's check that it is only one element in structure
                if len(structures) > 1:
                    print("We have multiple structure elements! ", dmrecord)

                # We have children
                # if this is a pdf compound object with '.pdfpage' children we need to handle
                # things differently
                j = 1
                for elem in structures[0]:
                    if elem.tag == "page":
                        has_pdfpage, page_id, pathinfo = get_page_info(elem)
                        if has_pdfpage:
                            download_pdf = True
                        else:
                            # this is a normal compound object
                            filename = str(pathinfo.name)
                            if filename in all_files_in_record:
                                logger.warning(
                                    "Record %s - The file name is duplicate! File name: %s"
                                    % (dmrecord, filename)
                                )
                            all_files_in_record.append(filename)
                            files_to_download.append(
                                (alias, page_id, output_path, filename)
                            )

                    elif elem.tag == "node":
                        for sub_elem in elem:
                            if sub_elem.tag == "page":
                                has_pdfpage, page_id, pathinfo = get_page_info(sub_elem)
                                if has_pdfpage:
                                    download_pdf = True
                                else:
                                    # this is a normal compound object
                                    filename = str(pathinfo.name)
                                    if filename in all_files_in_record:
                                        logger.warning(
                                            "Record %s - The file name is duplicate! File name: %s"
                                            % (dmrecord, filename)
                                        )
                                    all_files_in_record.append(filename)
                                    files_to_download.append(
                                        (alias, page_id, output_path, filename)
                                    )

                            elif sub_elem.tag == "node":
                                for sub_sub_elem in sub_elem:
                                    if sub_sub_elem.tag == "page":
                                        has_pdfpage, page_id, pathinfo = get_page_info(
                                            sub_sub_elem
                                        )
                                        if has_pdfpage:
                                            download_pdf = True
                                        else:
                                            # this is a normal compound object
                                            filename = str(pathinfo.name)
                                            if filename in all_files_in_record:
                                                logger.warning(
                                                    "Record %s - The file name is duplicate! File name: %s"
                                                    % (dmrecord, filename)
                                                )
                                            all_files_in_record.append(filename)
                                            files_to_download.append(
                                                (alias, page_id, output_path, filename)
                                            )

                if download_pdf:
                    # use the parent dmrecord to get the full pdf
                    filename = "{:06}_{:06}{}".format(int(dmrecord), 1, ".pdf")

                    files_to_download.append((alias, dmrecord, output_path, filename))

            else:
                # This is a single item
                filename = record.xpath("find")[0].text

                files_to_download.append((alias, dmrecord, output_path, filename))

        if files_to_download:

            pool = multiprocessing.Pool(processes=4)

            results = pool.starmap(download_file, files_to_download)
            pool.close()
            pool.join()
