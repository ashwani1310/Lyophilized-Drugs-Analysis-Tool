#pylint: disable=invalid-name
#pylint: disable=logging-format-interpolation
# -*- coding: utf-8 -*-
"""
.. :module:: dailymed
   :platform: Linux
   :synopsis: Module for fetching data from Dailymed through different
              API calls.

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 18, 2022)
"""

import json
import xmltodict
import datetime
import requests
import logger as log
from pymongo import (
    MongoClient
)
from bs4 import (
    BeautifulSoup
)
from download_drugs_data import (
    DownloadDrugsData
)
from database import (
    DrugsMetaCollection,
    IngredientsCollection,
    LyophilizedCollection
)
from connection import (
    RequestWrapper
)
from url_mapping import URL_MAP

LYOPHILIZED = "lyophilized"
DAILYMED_WEBPAGE_URL = URL_MAP.get('dailymed_webpage_url')

class DailyMed(object):
    """
    Class to retrieve data from DailyMed through API calls and
    perform different operations on the response.
    """

    def __init__(self):
        self.request_wrapper = RequestWrapper()
        self._drugs_db_obj = None
        self._ingredients_db_obj = None
        self._lyophilized_db_obj = None
    
    @property
    def drugs_db_obj(self):
        if not self._drugs_db_obj:
            self._drugs_db_obj = DrugsMetaCollection()
        return self._drugs_db_obj

    @property
    def ingredients_db_obj(self):
        if not self._ingredients_db_obj:
            self._ingredients_db_obj = IngredientsCollection()
        return self._ingredients_db_obj

     @property
    def lyophilized_db_obj(self):
        if not self._lyophilized_db_obj:
            self._lyophilized_db_obj = LyophilizedCollection()
        return self._lyophilized_db_obj

    def sanitize_list(self, data):
        if not isinstance(data, list):
            data_list = list()
            data_list.append(data)
            return data_list
        return data
    
    def get_spl_set_id(self, application_number):
        """Return the SPL (Structured Product Labeling) document's ID for
        the given drug's application number.

        :param application_number: (str) Application number for a drug.
        :returns setid_and_title: (list) Returns a list of dicts containing different
                                set ID and corresponding label for an application number.
        """
        page_number = 1
        page_size = 1000
        cursor = URL_MAP.get('get_spl_set_id')

        if not cursor:
            log.do_error(f"No API Url mapping present for retreiving SPL Set ID.")
            return
        cursor = cursor.format(application_number, page_number, page_size)

        setid_and_title = list()
        while cursor:
            try:
                response = self.request_wrapper.make_request('get', cursor)
                response_json = response.json()
            except Exception as exc:
                log.do_error(f"Failed to get SPL set ID for app number: {application_number}, error: {exc}")
                raise exc
            else:
                results = response_json.get('data', [])
                for result in results:
                    setid = result.get('setid')
                    title = result.get('title')
                    if not (setid and title):
                        log.do_error(f"Missing SetID: {setid} or title: {title} for app number: {application_number}")
                    else:
                        setid_and_title.append({'setid': setid, 'title': title})
                next_page_url = response_json.get('metadata', {}).get('next_page_url')
                if next_page_url == "null":
                    cursor = None
                else:
                    cursor = next_page_url
        
        return setid_and_title

    
    def update_drugs_setids_to_db(self, query=None):
        """Fetch SPL set ID for different drugs and update these setIDs to corresponding
        record for the drug (based on application number) in the `ingredients` collection.
        """
        ingredients_collection = list()
        search_query = {'set_ids': {'$exists': 0}}
        if query:
            search_query.update(query)
        for record in self.ingredients_db_obj.get_records(query=search_query):
            ingredients_collection.append({'_id': record.get('_id')})

        
        for record in ingredients_collection:
            app_number = record.get('_id')
            try:
                setid_and_title = self.get_spl_set_id(app_number)
            except Exception:
                continue

            set_ids_dict = dict()

            is_lyophilized = False
            for response in setid_and_title:
                setid = response.get("setid")
                title = response.get("title", "").lower()
                if LYOPHILIZED in title:
                    is_lyophilized = True

                dailymed_webpage_url = DAILYMED_WEBPAGE_URL.format(setid)
                set_ids_dict.update({
                    setid: {'title': title, 'web_url': dailymed_webpage_url}
                })

            if not setid_and_title:
                record.update({LYOPHILIZED: "N/A"})
            else:
                record.update({LYOPHILIZED: is_lyophilized})
            
            record.update({
                'set_ids': set_ids_dict
            })

        if ingredients_collection:
            self.ingredients_db_obj.bulk_update({'insert': ingredients_collection})
            log.do_info(f"Updated ingredients collection.")

    def insert_drugs_to_lyophilized_coll(self):
        """Fetches drugs with `lyophilized: True` key-value pair from ingredients collection
        and stores them in lyophilized collection where active and inactive ingredients will
        be stored for faster processing.
        """
        lyophilized_collection = list()
        record_counter = 0
        search_query = {LYOPHILIZED: True} 
        for record in self.ingredients_db_obj.get_records(query=search_query):
            lyophilized_collection.append(record)
            record_counter += 1
            if (record_counter % 5000) == 0:
                self.lyophilized_db_obj.bulk_update({'insert': lyophilized_collection})
                log.do_info(f"Updated lyophilized collection with {len(lyophilized_collection)} records.")
                lyophilized_collection = list()
        
        if lyophilized_collection:
            self.lyophilized_db_obj.bulk_update({'insert': lyophilized_collection})
            log.do_info(f"Updated lyophilized collection with {len(lyophilized_collection)} records.")


    def parse_ingredient(self, ingredient):
        """Retrun a dict with the required key value pairs for ingredients after 
        parsing the ingredient response retrieved from the API call.

        :param ingredient: (dict) Dict containing the ingredient metadata from the API call.
        :returns: (ingredient_meta) Dict that contains ingredient metadata we need to store in db.
        """
        strength_list = list()
        strength = ""
        quantity = ingredient.get('quantity', {})
        strength_list.append(quantity.get('numerator', {}).get('@value', ""))
        strength_list.append(quantity.get('numerator', {}).get('@unit', ""))
        strength_list.append("")
        strength_list.append(quantity.get('denominator', {}).get('@value', ""))
        strength_list.append(quantity.get('denominator', {}).get('@unit', ""))
        if any(strength_list):
            denominator = strength_list[3]+strength_list[4]
            if (not denominator) or (denominator == "11"):
                strength = "".join(strength_list[0:2])
            else:
                strength_list[2] = " in "
                strength = "".join(strength_list)

        substance = ingredient.get('ingredientSubstance', {})
        code = substance.get('code', {}).get('@code', "")
        name = substance.get('name', "").lower()

        return {'name': name, 'code': code, 'strength': strength}


    def get_ingredients(self, setid):
        """Fetch the ingreients info, via an API call, for a given drug (based on drug's SPL set ID).

        :param setid: (str) SPL set ID for a particular drug.
        :returns ingredients_info: (list) Returns a list of dicts containing different
                                active and inactive ingredients for the given setID.
        """

        url = URL_MAP.get('get_spl_document')
        if not url:
            log.do_error(f"No API Url mapping present for retreiving SPL Document.")
            return
        url = url.format(setid)

        active_ingredients_set = set()
        inactive_ingredients_set = set()
        active_ingredients_list = list()
        inactive_ingredients_list = list()
        try:
            response = self.request_wrapper.make_request('get', url)
            response_data = xmltodict.parse(response.content)
            response_json = json.loads(json.dumps(response_data))
        except Exception as exc:
            log.do_error(f"Failed to get SPL Document for setid: {setid}, error: {exc}")
            raise exc
        else:
            try:
                documents = response_json.get('document', {}).\
                                        get('component', {}).\
                                        get('structuredBody', {}).\
                                        get('component', [])
                iterator = 0
                lyophilized = False
                documents_list = self.sanitize_list(documents)
                display_name_to_ingredients = dict()
                for document in documents_list:
                    subjects = document.get('section', {}).get('subject', [])
                    subjects_list = self.sanitize_list(subjects)
                    ingredients = list()
                    for subject in subjects_list:
                        products = subject.get('manufacturedProduct', {}).get('manufacturedProduct', {})
                        ingredients = products.get('ingredient', [])
                        if ingredients:
                            display_name = products.get('formCode', {}).get('@displayName', "").lower()
                            display_name += ("_" + str(iterator))
                            iterator += 1
                            display_name_to_ingredients[display_name] = ingredients
                            if LYOPHILIZED in display_name:
                                lyophilized = True

                        else:
                            parts = products.get('part', [])
                            parts = self.sanitize_list(parts)
                            for part in parts:
                                part_products = part.get('partProduct', {})
                                display_name = part_products.get('formCode', {}).get('@displayName', "").lower()
                                display_name += ("_" + str(iterator))
                                iterator += 1
                                part_ingredients = part_products.get('ingredient', [])
                                part_ingredients = self.sanitize_list(part_ingredients)
                                display_name_to_ingredients[display_name] = part_ingredients
                                if LYOPHILIZED in display_name:
                                    lyophilized = True
                                #ingredients.extend(part_ingredients)
                                
                ingredients_list = list()
                remove_additional_fields = False
                if iterator > 1 and lyophilized:
                    remove_additional_fields = True
                    
                for key, value in display_name_to_ingredients.items():
                    if remove_additional_fields:
                        if LYOPHILIZED in key:
                            value = self.sanitize_list(value)
                            ingredients_list.extend(value)
                    else:
                        value = self.sanitize_list(value)
                        ingredients_list.extend(value)

                ingredients_list = self.sanitize_list(ingredients_list)
        
                for ingredient in ingredients_list:
                    ingredient_meta = self.parse_ingredient(ingredient)
                    ingredient_str = f"{ingredient_meta['name']}{ingredient_meta['code']}{ingredient_meta['strength']}"
                    if ingredient.get('@classCode', "").startswith("ACT"):                 
                        if ingredient_str not in active_ingredients_set:
                            active_ingredients_set.add(ingredient_str)
                            active_ingredients_list.append(ingredient_meta)

                    elif ingredient.get('@classCode', "").startswith("IACT"):
                        if ingredient_str not in inactive_ingredients_set:
                            inactive_ingredients_set.add(ingredient_str)
                            inactive_ingredients_list.append(ingredient_meta)

            except Exception as exception:
                log.do_error(f"Error while parsing SPL document for setid: {setid}, error: {exception}")

        return {'active': active_ingredients_list, 'inactive': inactive_ingredients_list}


    def update_ingredients_to_db(self, query=None):
        """Fetch Ingredeients info for different drugs and update the info to corresponding
        record for the drug (based on application number) in the `ingredients` collection.
        """
        setids_list = list()
        search_query = {LYOPHILIZED: True}
        if query:
            search_query.update(query)
        for record in self.lyophilized_db_obj.get_records(query=search_query):
            setids_list.append({'_id': record.get('_id'), 'set_ids': record.get('set_ids')})

        log.do_info(f"Number of lyophilized records in db: {len(setids_list)}")

        update_counter = 0

        for record in setids_list:
            active_ingredients = dict()
            inactive_ingredients = dict()
            active_ingredients_set = set()
            inactive_ingredients_set = set()
            set_ids = record.get('set_ids')
            for setid, _ in set_ids.items():
                try:
                    ingredients = self.get_ingredients(setid)
                except Exception:
                    continue

                if not ingredients:
                    continue
                active_ingredients.update({setid: ingredients.get('active')})
                inactive_ingredients.update({setid: ingredients.get('inactive')})

                for active in ingredients.get('active', []):
                    if active.get('name'):
                        active_ingredients_set.add(active.get('name'))

                for inactive in ingredients.get('inactive', []):
                    if inactive.get('name'):
                        inactive_ingredients_set.add(inactive.get('name'))

            record['active_ingredients_list'] = list(active_ingredients_set)
            record['inactive_ingredients_list'] = list(inactive_ingredients_set)

            if active_ingredients or inactive_ingredients:
                record['active_ingredients'] = active_ingredients
                record['inactive_ingredients'] = inactive_ingredients
                update_counter += 1

            if (update_counter % 50) == 0:
                log.do_info(f"Number of records processed till now: {update_counter}")

        self.lyophilized_db_obj.bulk_update({'insert': setids_list})
        log.do_info(f"Updated ingredients collection with ingredients info with {update_counter} records.")
        
    def get_lyophilized_drugs_from_dailymed(self, url):
        """Makes an HTTP call to get the web page for lyophilized drugs from 
        dailymed and uses BeautifulSoup to parse the web page.

        :param url: (str) HTTP url to make the GET request. 
        :returns set_ids: (set) Returns a set of SPL set IDs. SPL set ID is an UUID 
                                associated with a drug.
        """
        set_ids = set()
        try:
            response = self.request_wrapper.make_request('get', url)
            soup = BeautifulSoup(response.text, 'html.parser')

            attrs = {
                'class': "drug-info-link"
            }
            
            for drug in soup.find_all('a', attrs=attrs):
                link = drug.get('href', "")
                link_split = link.split('setid=')
                if len(link_split) > 1:
                    set_ids.add(link_split[1])
        except Exception as exc:
            log.do_error(f"Error while fetching lyophilized drugs from dailymed, error: {exc}")
        
        return set_ids

    def lyophilized_setid_from_dailymed(self):
        """Calls underlying function `get_lyophilized_drugs_from_dailymed` to get the
        lyophilized drugs from Dailymed. It returns the list of SPL set ID of drugs
        which contain the keyword `lyophilized` in its `description` section or in the
        `dosage forms and strength` section on the drugs's dailymed page.
        
        :returns setids_list: (list) Returns a list of SPL set IDs. SPL set ID is an UUID 
                                     associated with a drug.
        """
        
        api_list = list()
        api_list.append(URL_MAP.get('lyophilized_from_dosage'))
        api_list.append(URL_MAP.get('lyophilized_from_description'))

        setids_set = set()
        for api in api_list:
            page_number = 1
            while True:
                url = api.format(page_number)
                setids = self.get_lyophilized_drugs_from_dailymed(url)
                if not setids:
                    break
                else:
                    setids_set.update(setids)
                page_number += 1

        setids_list = list(setids_set)
        setids_list_len = len(setids_list)
        log.do_info(f"Total number of lyophilized drugs returned by dailymed: {setids_list_len}")
    
        return setids_list

    
    def compare_dailymed_data_from_db(self, setids=None):
        """Checks what setIds are not marked as Lyophilized in mongo collection
        based on the setIds for the lyophilized drugs retrieved from dailymed.

        :returns ids_dict: (dict) Returns a dict containing the mongo collection _id
                            with the value as the list of set ids that are lyophilized
                            as per dailymed but not marked as one in mongo collection.
        """
        if not setids:
            setids = self.lyophilized_setid_from_dailymed()

        drug_counter = 0
        lyo_drugs_counter = 0
        ids_dict = dict()
        for setid in setids:
            try:
                query = {"set_ids.{}".format(setid): {'$exists': 1}}
                for record in self.lyophilized_db_obj.get_records(query=query):
                    drug_counter += 1
                    if not record.get(LYOPHILIZED):
                        if not ids_dict.get(record.get('_id')):
                            ids_dict[record.get('_id')] = list()
                        ids_dict[record.get('_id')].append(setid)
                        lyo_drugs_counter += 1
            except Exception as exc:
                log.do_error(f"Error while fetching drug info from db for: {setid}, error: {exc}")

        log.do_info(f"Lyophilized drugs from dalymed: {len(setids)}, drugs present in db: {drug_counter}")
        log.do_info(f"Drugs not marked as lyophilized in db: {lyo_drugs_counter}")

        records_list = list()
        update_counter = 0
        for key, _ in ids_dict.items():
            records_list.append({'_id': key, LYOPHILIZED: True})
            update_counter += 1

        self.lyophilized_db_obj.bulk_update({'insert': records_list})
        self.ingredients_db_obj.bulk_update({'insert': records_list})

        log.do_info(f"Number of records updated as lyophilized in mongo collections: {update_counter}")

        return ids_dict

    def get_ingredients_for_additional_drugs(self, ids_dict):
        """Given a dict of `_id` of mongo records, and the corresponding SPL Set IDs
        in that record, get active and inactive ingredients for set IDs update in 
        mongo collections.

        :param ids_dict: (dict) Dict of mongo `_id` and the corresponding SPL set ID
                         for that mongo record. 
        """
       
        update_counter = 0
        records_to_insert = list()
        for key, value in ids_dict.items():
            try:
                db_record = {'_id': key}
                
                active_ingredients = dict()
                inactive_ingredients = dict()
                active_ingredients_set = set()
                inactive_ingredients_set = set()
                for setid in value:
                    log.do_info(f"Updating record for _id: {key} and setid: {setid}")
                    try:
                        ingredients = self.get_ingredients(setid)
                        db_record[LYOPHILIZED] = True
                    except Exception:
                        continue

                    if not ingredients:
                        continue
                    active_ingredients.update({setid: ingredients.get('active')})
                    inactive_ingredients.update({setid: ingredients.get('inactive')})

                    for active in ingredients.get('active', []):
                        if active.get('name'):
                            active_ingredients_set.add(active.get('name'))

                    for inactive in ingredients.get('inactive', []):
                        if inactive.get('name'):
                            inactive_ingredients_set.add(inactive.get('name'))

                db_record['active_ingredients_list'] = list(active_ingredients_set)
                db_record['inactive_ingredients_list'] = list(inactive_ingredients_set)

                if active_ingredients or inactive_ingredients:
                    db_record['active_ingredients'] = active_ingredients
                    db_record['inactive_ingredients'] = inactive_ingredients
                    update_counter += 1

                records_to_insert.append(db_record)

            except Exception as exc:
                log.do_error(f"Error while fetching and updating ingredients for _id: {key}, error: {exc}")
            
        if records_to_insert:
            self.lyophilized_db_obj.bulk_update({'insert': records_to_insert})
        
        log.do_info(f"Updated additional records in ingredients collection with {len(records_to_insert)} records.")