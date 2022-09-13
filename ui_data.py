import traceback
import logger as log
import pandas as pd
from datetime import (
    datetime
)
from database import (
    DrugsMetaCollection,
    IngredientsCollection,
    LyophilizedCollection
)
from dash import (
    html,
    dcc
)

class MongoData(object):

    def __init__(self):
        self._ingredients_db_obj = None
        self._lyophilized_db_obj = None

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

    def sanitize_list(self, item):
        if not isinstance(item, list):
            result = list()
            result.append(item)
            return result
        return item

    def get_table_data(self, start_date=None, end_date=None, product_search=None, active_search=None, inactive_search=None):
        
        search_query = dict()
        result = list()
        if product_search:
            result.extend(self.sanitize_list(product_search))
            search_query.update({
                'products': {'$in': result}
            })
        elif active_search:
            result.extend(self.sanitize_list(active_search))
            search_query.update({
                'active_ingredients_list': {'$in': result}
            })
        elif inactive_search:
            result.extend(self.sanitize_list(inactive_search))
            search_query.update({
                'inactive_ingredients_list': {'$in': result}
            })
        
        if not (start_date or search_query):
            today = datetime.today()
            start_date = datetime(today.year, today.month, 1)

            ##
            ## For Testing purposes, using start date from Jan 1, 2022.
            ##
            start_date = datetime(2022, 1, 1)
            search_query.update({'date': {'$gte': start_date}})
        if end_date:
            search_query['date'].update({
                '$lt': end_date
            })       

        records_rows = list()
        record_id = None
        try:
            for records in self.lyophilized_db_obj.get_records(query=search_query):
                record_id = records.get('_id')
                labels_rows = list()
                active_ingredients_rows = list()
                active_strength_rows = list()
                inactive_ingredients_rows = list()
                inactive_strength_rows = list() 
                for setid, labels in records.get('set_ids', {}).items():
                    row = list()
                    active = records.get('active_ingredients', {}).get(setid, [])
                    inactive = records.get('inactive_ingredients', {}).get(setid, [])
                    active_len = len(active)
                    inactive_len = len(inactive)
                    row_span = max(active_len, max(inactive_len, 0))

                    if row_span == 0:
                        continue

                    row.append(dcc.Markdown(f"[{labels.get('title')}]({labels.get('web_url')})"))
                    row.append(row_span)

                    labels_rows.append(row)
                    for i in range(1, row_span):
                        labels_rows.append(list())

                    
                    for i in range(0, row_span):
                        active_name_row = ["", 1]
                        active_strength_row = ["", 1]
                        inactive_name_row = ["", 1]
                        inactive_strength_row = ["", 1]

                        if i < active_len:
                            active_name_row = [active[i].get('name', ""), 1]
                            active_strength_row = [active[i].get('strength', ""), 1]
                        
                        if i < inactive_len:
                            inactive_name_row = [inactive[i].get('name', ""), 1]
                            inactive_strength_row = [inactive[i].get('strength', ""), 1]
                        
                        active_ingredients_rows.append(active_name_row)
                        active_strength_rows.append(active_strength_row)
                        inactive_ingredients_rows.append(inactive_name_row)
                        inactive_strength_rows.append(inactive_strength_row)


                total_span = len(labels_rows)
                if total_span <= 0:
                    total_span = 1

                app_number = records.get('application_number')
                app_list = [app_number, total_span]
            
                product_outer_list = list()     
                for product in records.get('products', []):
                    product_rows = [product.upper(), html.Br()]
                
                product_rows.append(app_number)
                product_outer_list = [product_rows, total_span, "products"]

                date_rows = list() 
                if not start_date:
                    date_value = list()
                    for date in records.get('date'):
                        date_value.append(date.strftime('%m-%d-%Y'))
                        date_value.append(html.Br())
                else:
                    date_value = ""
                    for date in records.get('date'):
                        if date.year >= start_date.year and date.month >= start_date.month:
                            if end_date: 
                                if date.year <= end_date.year and date.month <= end_date.month:
                                    date_value = date.strftime('%m-%d-%Y')
                                    break
                            else:
                                date_value = date.strftime('%m-%d-%Y')
                                break
                
                date_rows = [date_value, total_span, "dates"]
                company_rows = [records.get('company'), total_span]
            
                current_row = [date_rows, product_outer_list, company_rows]

                for i in range(0, len(labels_rows)):
                    if labels_rows[i]:
                        current_row.append(labels_rows[i])
                    current_row.extend([active_ingredients_rows[i], active_strength_rows[i]])
                    current_row.extend([inactive_ingredients_rows[i], inactive_strength_rows[i]])
    
                    records_rows.append(current_row)
                    current_row = list()

        except Exception as exc:
            log.do_error(f"Exception occurred while fetching records from database for record: {record_id}, error: {str(exc)}, traceback: {traceback.format_exc()}")

        return records_rows


    def get_search_bar_data(self):
        products = set()
        active_ingredients = set()
        inactive_ingredients = set()
        for records in self.lyophilized_db_obj.get_records():
            products.update(set(records.get('products', [])))
            active_ingredients.update(set(records.get('active_ingredients_list', [])))
            inactive_ingredients.update(set(records.get('inactive_ingredients_list', [])))

        return (list(products), list(active_ingredients), list(inactive_ingredients))

    
    def generate_occurences_data(self):
        try:
            active_ingredients_dict = dict()
            inactive_ingredients_dict = dict() 
            products_set = set()       
            for record in self.lyophilized_db_obj.get_records():
                products = record.get('products', [])
                active_ingredients = record.get('active_ingredients_list', [])
                inactive_ingredients = record.get('inactive_ingredients_list', [])
                        
                date = record.get('date', [])[0]
                date_value = date.strftime('%Y')

                for product in products:
                    for ingredient in active_ingredients:
                        id = product + ingredient
                        if id not in products_set:
                            products_set.add(id)
                            existing_val = active_ingredients_dict.get(ingredient, {}).get(date_value, 0)
                            total_val = active_ingredients_dict.get(ingredient, {}).get('total_count', 0)
                            if active_ingredients_dict.get(ingredient):
                                active_ingredients_dict[ingredient].update({date_value: existing_val + 1})
                                active_ingredients_dict[ingredient].update({'total_count': total_val + 1})
                            else:
                                active_ingredients_dict[ingredient] = {}
                                active_ingredients_dict[ingredient].update({date_value: 1})
                                active_ingredients_dict[ingredient].update({'total_count': 1})

                    for ingredient in inactive_ingredients:
                        id = product + ingredient
                        if id not in products_set:
                            products_set.add(id)
                            existing_val = inactive_ingredients_dict.get(ingredient, {}).get(date_value, 0)
                            total_val = inactive_ingredients_dict.get(ingredient, {}).get('total_count', 0)
                            if inactive_ingredients_dict.get(ingredient):
                                inactive_ingredients_dict[ingredient].update({date_value: existing_val + 1})
                                inactive_ingredients_dict[ingredient].update({'total_count': total_val + 1})
                            else:
                                inactive_ingredients_dict[ingredient] = {}
                                inactive_ingredients_dict[ingredient].update({date_value: 1})
                                inactive_ingredients_dict[ingredient].update({'total_count': 1})
        except Exception as exc:
            log.do_error(f"Exception occurred while fetching ingredients from Database, error: {str(exc)}")

        return {'active': active_ingredients_dict, 'inactive': inactive_ingredients_dict}


    def get_timeseries_dataframe(self):

        time_series_data = self.generate_occurences_data()
        inactive_list = list()
        inactive_ing_list = list()
        active_list = list()
        active_ing_list = list()
        inactive_dates_list = list()
        active_dates_list = list()
        inactive_occ_list = list()
        active_occ_list = list()
        active_total_count = list()
        inactive_total_count = list()

        for ingredient, value in time_series_data.get('active', {}).items():
            active_ing_list.append(ingredient)
            for key, count in value.items():
                if key == "total_count":
                    active_total_count.append(count)
                else:
                    active_dates_list.append(int(key))
                    active_list.append(ingredient)
                    active_occ_list.append(count)


        for ingredient, value in time_series_data.get('inactive', {}).items():
            inactive_ing_list.append(ingredient)
            for key, count in value.items():
                if key == "total_count":
                    inactive_total_count.append(count)
                else:
                    inactive_dates_list.append(int(key))
                    inactive_list.append(ingredient)
                    inactive_occ_list.append(count)

        active_time_series = pd.DataFrame({
            "Ingredient": active_list,
            "Year": active_dates_list,
            "Occurences": active_occ_list
        })

        inactive_time_series = pd.DataFrame({
            "Ingredient": inactive_list,
            "Year": inactive_dates_list,
            "Occurences": inactive_occ_list
        })

        active_chart = pd.DataFrame({
            "Ingredient": active_ing_list,
            "Occurences": active_total_count
        })

        inactive_chart = pd.DataFrame({
            "Ingredient": inactive_ing_list,
            "Occurences": inactive_total_count
        })
    
        active_time_series = active_time_series.sort_values(by=['Year'])
        inactive_time_series = inactive_time_series.sort_values(by=['Year'])
        active_chart = active_chart.sort_values(by=['Occurences'])
        inactive_chart = inactive_chart.sort_values(by=['Occurences'])

        return (active_chart, inactive_chart, active_time_series, inactive_time_series)