#pylint: disable=invalid-name
#pylint: disable=logging-format-interpolation
# -*- coding: utf-8 -*-
"""
.. :module:: fda_data_translator
   :platform: Linux
   :synopsis: Module for parsing the downloada FDA Drugs Data
              to fetch meta data and store in a database.

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 16, 2022)
"""

import json
import datetime
import logger as log
from pymongo import (
    MongoClient
)
from download_drugs_data import (
    DownloadDrugsData
)
from database import (
    DrugsMetaCollection,
    IngredientsCollection,
    LyophilizedCollection
)

class DrugsMeta(object):

    def __init__(self):
        self._drugs_data_obj = None

    @property
    def drugs_data_obj(self):
        if not self._drugs_data_obj:
            self._drugs_data_obj = DownloadDrugsData()
        return self._drugs_data_obj

    def _get_drugs_data(self):
        self.drugs_data_obj.download_data()

    def load_drugs_data(self, download_new_data=True):
        if download_new_data:
            self._get_drugs_data()
        
        drugs_data = {}     
        try:
            with open(self.drugs_data_obj.drugs_data_file) as fileobj:
                drugs_data = json.load(fileobj)
        except Exception as exc:
            log.do_error(f"Error while loading drugs data from json file "
                            f"{str(exc)}")
            raise exc
        else:
            return drugs_data

    def get_drugs_meta_json(self, drug_meta):

        drug_meta_json = {}
        try:
            application_number = drug_meta.get("application_number")
            sponsor_name = drug_meta.get("sponsor_name", "").lower()
            
            products_name = set()
            for product in drug_meta.get("products", []):
                prod_name = product.get("brand_name", "").lower()
                if prod_name:
                    products_name.add(prod_name)

            date_list = set()
            for submission in drug_meta.get("submissions", []):
                date_list.add(submission.get("submission_status_date", ""))

            if not date_list:
                log.do_error(f"Submission dates not present for record, app number: {application_number}")
                return drug_meta_json

            date_list = list(date_list)
            date_list.sort()
            datetime_list = list()
            for date in date_list:
                date.strip()
                year = date[0:4]
                month = date[4:6]
                day = date[6:8]
                if year and month and day:
                    datetime_list.append(datetime.datetime(int(year), int(month), int(day)))
                else:
                    log.do_error(f"Submission date: {date} not in proper format for app number: {application_number}")

            drug_meta_json.update({
                "application_number": application_number,
                "company": sponsor_name,
                "products": list(products_name),
                "date": datetime_list
            })
        except Exception as exc:
            log.do_error(f"Error while parsing drugs meta, error: {str(exc)}")
            raise exc

        return drug_meta_json

    def update_drugs_data_to_db(self, download_new_data=True):
        drugs_data = self.load_drugs_data(download_new_data)
        drug_results = drugs_data.get("results", {})
        update_counter = 0

        db_obj = IngredientsCollection()
        records_list = list()
        for drug_meta in drug_results:
            drug_meta_json = self.get_drugs_meta_json(drug_meta)
            if drug_meta_json:
                records_list.append(drug_meta_json)
                update_counter += 1
            if (update_counter % 10000 == 0) and records_list:
                db_obj.bulk_update({'insert': records_list})
                records_list = list()
    
        if records_list:
            db_obj.bulk_update({'insert': records_list})
            records_list = list()
        
        log.do_info(f"Updated {update_counter} records in DrugsMeta collection.")