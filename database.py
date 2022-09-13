# -*- coding: utf-8 -*-
"""
.. :module:: database
   :platform: Linux
   :synopsis: Common database utilities.

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 14, 2022)
"""

from functools import wraps
import time
import json
import logger as log
from pymongo import (
    MongoClient,
    UpdateOne,
    DeleteOne
)
from pymongo.errors import (
    ConnectionFailure,
    PyMongoError,
    BulkWriteError,
)

MAX_RETRIES = 20

CONFIG_FILE = "config"

def get_config():
    """
    JSON Load the config from config file.

    :return: (dict) Contents of the config file.
    """
    config = dict()
    with open(CONFIG_FILE) as fp:
        try:
           config = json.load(fp)
        except Exception as exception:
            log.do_error(f"Error getting config " 
                            f"{str(exception)}")
            raise exception

    return config

def singleton(class_):
    """
    Decorator to make the underlying class singleton, that is allow
    only once instance creation for the class.

    :param class_: (class) Reference to the class.
    """
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

def db_retry(retry_count=MAX_RETRIES):
    """
    Decorator to retry db exceptions like ConnectionFailure and raise exceptions
    for PyMongo failures.

    :param retry_count: (int) Number of retries to be performed.
    """
    def retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except (ConnectionFailure, BulkWriteError) as exception:
                    if retry > retry_count:
                        log.do_error(f"Failed to perform db operation, error: {str(exception)}")
                        raise exception
                    else:
                        retry += 1
                        log.do_error(f"Retrying db operation, retry_count: {retry}, error: {str(exception)}")
                except PyMongoError as exception:
                    log.do_error(f"PyMongo error while performing db operation, error: {str(exception)}")
                    raise exception

        return wrapper
    return retry


@singleton
class DatabaseConnection(object):
    """
    Class to establish database connection. DB's host and port
    are read from the `config` file. Default host and port for
    mongo db are `localhost` and `27017` respectively.
    """
    def __init__(self):
        self._db_client = None

    def get_db_client(self):
        """
        Establish db client connection and return the connection object.
        Retry for a max value of `MAX_RETRIES`.

        :return: (object) Database connection object.
        :raises: (Exception) In case of Database connection failure.
        """
        retry = 1
        while True:
            try:
                if not self._db_client:
                    config = get_config()
                    self._db_client = MongoClient(config.get("db_host") or "localhost",
                                                config.get("db_port") or 27017,
                                                serverSelectionTimeoutMS = 10000)
            except errors.ServerSelectionTimeoutError as connection_error:
                raise connection_error
            except Exception as exception:
                if retry > MAX_RETRIES:
                    log.do_error(f"Failed to get db client connection, error: {str(exception)}")
                    raise exception
                else:
                    retry += 1
                    log.do_error(f"Retrying to get db client connection, retry_count: {retry}, error: {str(exception)}")
            else:
                break

        return self._db_client

class Databases:
    LYOHUB_DB = "lyohub"
    DRUGS_META_COLLECTION = "drugs_meta"
    INGREDIENTS_COLLECTION = "ingredients"
    LYOPHILIZED_COLLECTION = "lyophilized"

class DrugsMetaCollection(object):
    """
    Class to perform different operations on `drugs_meta` collection
    in Mongo DB.
    """
    def __init__(self):
        db = DatabaseConnection()
        db_client = db.get_db_client()
        self.db_connection = db_client[Databases.LYOHUB_DB][Databases.DRUGS_META_COLLECTION]

    @db_retry() #decorator
    def bulk_update(self, records):
        """Does a unordered bulk upsert and delete of given records.

        :param records: (dict) Dict containg two fields, `insert` for records to add,
                        and `delete` for records to delete.
        :returns: (BulkWriteResult) Type and count of operations performed if any operations to perform 
                                    else None if there are no operations to perform.
        """
        operations = []
        records_to_insert = records.get('insert', [])
        records_to_delete = records.get('delete', [])

        for record in records_to_insert:
            record['lut'] = int(time.time())
            operations.append(UpdateOne({"application_number": record.get("application_number")}, 
                                        {"$set" : record}, upsert=True))

        for record in records_to_delete:
            operations.append(DeleteOne(record))

        if not operations:
            return

        return self.db_connection.bulk_write(operations, ordered=False)

    @db_retry(retry_count=10)
    def get_records(self, query=None):
        """Ftech records from DB. If no explicit query is given then return all the records.

        :param query: (dict) Dict containing the query to be performed on find operation on db.
        """
        if query:
            return self.db_connection.find(query)

        return self.db_connection.find()


class IngredientsCollection(object):
    """
    Class to perform different operations on `ingredients` collection
    in Mongo DB.
    """
    def __init__(self):
        db = DatabaseConnection()
        db_client = db.get_db_client()
        self.db_connection = db_client[Databases.LYOHUB_DB][Databases.INGREDIENTS_COLLECTION]

    @db_retry()
    def bulk_update(self, records):
        """Does a unordered bulk upsert and delete of given records.

        :param records: (dict) Dict containg two fields, `insert` for records to add,
                        and `delete` for records to delete.
        :returns: (BulkWriteResult) Type and count of operations performed if any operations to perform 
                                    else None if there are no operations to perform.
        """
        operations = []
        records_to_insert = records.get('insert', [])
        records_to_delete = records.get('delete', [])

        for record in records_to_insert:
            record['lut'] = int(time.time())
            operations.append(UpdateOne({"_id": record.get("_id")}, 
                                        {"$set" : record}, upsert=True))

        for record in records_to_delete:
            operations.append(DeleteOne(record))

        if not operations:
            return

        return self.db_connection.bulk_write(operations, ordered=False)

    @db_retry(retry_count=10)
    def get_records(self, query=None):
        """Ftech records from DB. If no explicit query is given then return all the records.

        :param query: (dict) Dict containing the query to be performed on find operation on db.
        """
        if query:
            return self.db_connection.find(query)

        return self.db_connection.find()

    @db_retry(retry_count=10)
    def remove_keys(self, query, keys_to_remove):
        """Remove keys from records based on the query and the keys to remove from records.

        :param query: (dict) Dict containing the query to be performed on updateMany operation on db.
        :param keys_to_remove: (dict) Dict containing the keys to remove from records.
        """

        modified_records = self.db_connection.update_many(query, keys_to_remove)
        log.do_info(f"Number of documents updated with remove keys call: {modified_records.modified_count}")


class LyophilizedCollection(object):
    """
    Class to perform different operations on `lyophilized` collection
    in Mongo DB.
    """
    def __init__(self):
        db = DatabaseConnection()
        db_client = db.get_db_client()
        self.db_connection = db_client[Databases.LYOHUB_DB][Databases.LYOPHILIZED_COLLECTION]

    @db_retry()
    def bulk_update(self, records):
        """Does a unordered bulk upsert and delete of given records.

        :param records: (dict) Dict containg two fields, `insert` for records to add,
                        and `delete` for records to delete.
        :returns: (BulkWriteResult) Type and count of operations performed if any operations to perform 
                                    else None if there are no operations to perform.
        """
        operations = []
        records_to_insert = records.get('insert', [])
        records_to_delete = records.get('delete', [])

        for record in records_to_insert:
            record['lut'] = int(time.time())
            operations.append(UpdateOne({"_id": record.get("_id")}, 
                                        {"$set" : record}, upsert=True))

        for record in records_to_delete:
            operations.append(DeleteOne(record))

        if not operations:
            return

        return self.db_connection.bulk_write(operations, ordered=False)

    @db_retry(retry_count=10)
    def get_records(self, query=None):
        """Ftech records from DB. If no explicit query is given then return all the records.

        :param query: (dict) Dict containing the query to be performed on find operation on db.
        """
        if query:
            return self.db_connection.find(query).sort('date')

        return self.db_connection.find().sort('date')