# QuickStart guide

1. Install the required packages from the requirement.txt file.

2. Update the parameters "ALIAS" and "FILE_URL" to point to the correct CONTENTdm collection and the correct server.

3. Update the parameter "REL_PATH" to specify the local path to save the output.

4. Run `python contentdm_record_exporter.py` to export the records from CONTENTdm.

5. Run  `python contentdm_file_exporter.py` to export the files from CONTENTdm. This require the records to have been exported first.



# Credit
This script is inspired by the following work: https://github.com/UNC-Libraries/cdm-metadata-extractor
Thank you for sharing!

