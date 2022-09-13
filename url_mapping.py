# -*- coding: utf-8 -*-
"""
.. :module:: url_mapping
   :platform: Linux
   :synopsis: Module for mapping key to API url.

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 15, 2022)
"""

URL_MAP = {
    'download_fda_drugs_data': 'https://download.open.fda.gov/drug/drugsfda/drug-drugsfda-0001-of-0001.json.zip',
    'get_spl_set_id': 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?application_number={}&page={}&pagesize={}',
    'get_spl_document': 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/{}.xml',
    'dailymed_webpage_url': 'https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={}',
    'lyophilized_from_dosage': 'https://dailymed.nlm.nih.gov/dailymed/search.cfm?adv=1&labeltype=all&query=43678-2%3A%28lyophilized%29+&page={}&pagesize=200',
    'lyophilized_from_description': 'https://dailymed.nlm.nih.gov/dailymed/search.cfm?adv=1&labeltype=all&pagesize=200&page={}&query=34089-3%3A%28lyophilized%29+'
}