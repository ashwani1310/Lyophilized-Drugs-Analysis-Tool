#pylint: disable=invalid-name
#pylint: disable=logging-format-interpolation
# -*- coding: utf-8 -*-
"""
.. :module:: download_drugs_data
   :platform: Linux
   :synopsis: Module for downloading FDA Drugs Data

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 15, 2022)
"""

import os
import shutil
import zipfile
import urllib.request as request
import logger as log
from pathlib import (
    Path
)
from url_mapping import URL_MAP

class DownloadDrugsData(object):

    def __init__(self):
        self._folder_path = None
        self._temp_zip_file = None
        self._drugs_data_file = None

    @property
    def folder_path(self):
        if not self._folder_path:
            current_dir = Path(__file__).parent.absolute()
            self._folder_path = current_dir / 'drugsData'
        return self._folder_path

    @property
    def temp_zip_file(self):
        if not self._temp_zip_file:
            self._temp_zip_file = self.folder_path / 'tempDrugsData.zip'
        return self._temp_zip_file

    @property
    def drugs_data_file(self):
        if not self._drugs_data_file:
            self._drugs_data_file = self.folder_path / 'fdaDrugsData.json'
        return self._drugs_data_file
  
    def create_folder(self):
        try:
            if not self.folder_path.exists():
                os.makedirs(self.folder_path)
        except Exception as exc:
            log.do_error(f"Failed to create the directory folder " 
                                 f"{str(exc)}")

    def extract_zip_file(self):
        try:
            with zipfile.ZipFile(self.temp_zip_file) as zip_dir:
                zip_files = zip_dir.infolist()
                for zip_file in zip_files:
                    zip_file_name = zip_file.filename
                    zip_dir.extract(zip_file, self.folder_path)
                    os.rename(self.folder_path / zip_file_name, self.drugs_data_file)
            
            os.remove(self.temp_zip_file)
        except Exception as exc:
            log.do_error(f"Failed to extract the zip files " 
                                 f"{str(exc)}")
            raise exc
        else:
            log.do_info(f"Zip file successfully extracted.")


    def download_data(self):
        try:
            self.create_folder()
            url = URL_MAP.get('download_fda_drugs_data')
            if not url:
                log.do_error(f"No API Url mapping present for downloading FDA Drugs Data.")
                return 
            with request.urlopen(url) as infile, open(self.temp_zip_file, 'wb') as outfile:
                shutil.copyfileobj(infile, outfile)
            self.extract_zip_file()
        except Exception as exc:
            log.do_error(f"Failed to download FDA Drugs Data through API call " 
                                 f"{str(exc)}")
            raise exc
        else:
            log.do_info(f"FDA Drugs Data Download complete.")