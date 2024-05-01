import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure,AutoReconnect

def connect_to_mongodb(database_name:str):
    """
    The function `connect_to_mongodb` establishes a connection to a MongoDB database using the provided
    database name and connection string from environment variables.
    
    :param database_name: The `database_name` parameter in the `connect_to_mongodb` function is a string
    that represents the name of the MongoDB database to which you want to connect. When calling this
    function, you should provide the name of the database you want to work with as a string
    :type database_name: str
    :return: The function `connect_to_mongodb` returns two values: `db` which is the database object for
    the specified database name, and `client` which is the MongoDB client object used to interact with
    the database.
    """
    connection_string = os.getenv('MONGOPH_MONGODB_CONNECTION_STRING')
    if not connection_string:
        raise ValueError('MongoDB connection string not found in environment variable. set it with : MONGOPH_MONGODB_CONNECTION_STRING ')

    try:
        client = MongoClient(connection_string)
        db = client[database_name]
        client.server_info()
        return db,client
    except Exception as e:
        raise ValueError(f'Failed to connect to MongoDB: {e}')

def close_mongodb_connection(client:MongoClient):
    """
    The function `close_mongodb_connection` takes a `MongoClient` object as input and closes the
    connection if the object is not `None`.
    
    :param client: The `client` parameter in the `close_mongodb_connection` function is expected to be
    an instance of `MongoClient`, which is typically used to establish a connection to a MongoDB
    database
    :type client: MongoClient
    """
    if client:
        client.close()


def next_record_identifier_value(database, collection):
    """
    The function `next_record_identifier_value` generates and returns an incrementor ID for a given collection in a
    MongoDB database, incrementing the tally value in the incrementor collection.
    
    :param database: The `database` parameter in the `next_record_identifier_value` function refers to the name of
    the MongoDB database you want to connect to. This function is designed to generate an incrementor ID
    for a specific collection within the specified database. The function attempts to connect to the
    MongoDB database, increments the tally value
    :param collection: The `collection` parameter in the `next_record_identifier_value` function refers to the name
    of the collection in the MongoDB database for which you want to generate or increment an ID. This
    function is designed to connect to a MongoDB database, retrieve the current tally value for the
    specified collection from an 'incrementors
    :return: The function `next_record_identifier_value` returns the incremented value of the tally field for the
    specified collection in the MongoDB database. If the incrementor document for the collection exists,
    it retrieves the current tally value, increments it by 1, and returns the updated value. If the
    incrementor document does not exist, it initializes a new document with a tally value of 1 and
    returns 1.
    """
    try:
        db, client = connect_to_mongodb(database)

        incrementor_collection = os.getenv('MONGOPH_INCREMENTOR_COLLECTION', 'incrementors')

        incrementor = db[incrementor_collection].find_one_and_update(
            {'_id': collection},
            {'$inc': {'tally': 1}},
            projection={'tally': True},
            return_document=True
        )

        if incrementor:
            print(f"Generated incrementor ID for {collection}: {incrementor['tally']}")
            close_mongodb_connection(client)
            return incrementor['tally']
        else:
            db[incrementor_collection].insert_one({'_id': collection, 'tally': 1})
            close_mongodb_connection(client)
            print(f"Initialized incrementor ID for {collection}: 1")
            return 1
    except ConnectionFailure as e:
        raise ValueError(f"Database connection failure: {e}")
    except OperationFailure as e:
        raise ValueError(f"Database operation failure: {e}")
    finally:
        close_mongodb_connection(client)


def get_reference_key() :
    """
    The function `get_reference_key` returns the value of the environment variable
    'MONGOPH_REFERENCE_KEY' or 'record_id' if the environment variable is not set.
    :return: The function `get_reference_key()` is returning the value of the environment variable
    'MONGOPH_REFERENCE_KEY' if it is set, otherwise it is returning the default value 'record_id'.
    """
    return os.getenv('MONGOPH_REFERENCE_KEY', 'record_id')

def collection_exist(database,collection,create_it=False):
    """
    The function `collection_exist` checks if a collection exists in a MongoDB database and optionally
    creates it if it doesn't.
    
    :param database: The `database` parameter in the `collection_exist` function is a string that
    represents the name of the MongoDB database where the collection is located or should be created
    :param collection: The `collection` parameter in the `collection_exist` function refers to the name
    of the collection within the specified database that you want to check for existence
    :param create_it: The `create_it` parameter in the `collection_exist` function is a boolean flag
    that determines whether the collection should be created if it does not already exist in the
    specified database. If `create_it` is set to `True`, the function will attempt to add the collection
    to the database if it, defaults to False (optional)
    :return: The function `collection_exist` returns a boolean value indicating whether the specified
    collection exists in the given database. If the `create_it` parameter is set to `True` and the
    collection does not exist, it will attempt to create the collection before returning the result.
    """
    try:
        db, client = connect_to_mongodb(database)
        in_collection_list =  collection in db.list_collection_names()
        if not in_collection_list and create_it:
            add_collection(database,collection)
        close_mongodb_connection(client)
        return in_collection_list 
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
        raise ValueError('Database operation faillure: {e}')


def add_collection(database,collection):
    """
    The function `add_collection` attempts to retrieve the next record identifier value from a database
    collection and returns a boolean indicating whether the operation was successful.
    
    :param database: The `database` parameter in the `add_collection` function is typically a reference
    to the database where the collection will be added. This could be a connection object, database
    name, or any other identifier that allows the function to interact with the database
    :param collection: The `collection` parameter in the `add_collection` function represents the name
    of the collection in the database where you want to add a new record. It is the specific group of
    documents within a database that is used to store related data
    :return: The function `add_collection` is returning a boolean value based on the result of
    `next_record_identifier_value(database, collection)`. If the `next_record_identifier_value` function
    returns a truthy value (i.e., not 0), then the function will return `True`. Otherwise, it will
    return `False`.
    """
    try:
        tally = next_record_identifier_value(database,collection) #to 1 but with no record ! update it later to have 0 as first value
        return bool(tally)
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
        raise ValueError('Database operation faillure: {e}')


def create_index_on_collection(database,collection, index_name):
    """
    The function creates a text index on a specified collection in a MongoDB database.
    
    :param database: The `database` parameter is the name of the MongoDB database where the collection
    is located
    :param collection: The `collection` parameter in the `create_index_on_collection` function refers to
    the name of the collection in the MongoDB database on which you want to create an index. It is the
    specific subset of data within the database where you want to add the index
    :param index_name: The `index_name` parameter in the `create_index_on_collection` function is the
    name of the index that you want to create on a specific field in a MongoDB collection
    :return: True
    """
    try:
        db, client = connect_to_mongodb(database)
        indexes = db[collection].index_information()
        index_key = f'{collection}_{index_name}'
        if not indexes.get(index_key, None):
            db[collection].create_index([(index_name, 'text')], name=index_key)
        close_mongodb_connection(client)
        return True
    except Exception as e:
        raise ValueError('Database operation faillure: {e}')
    
def check_index_on_collection(database,collection, index_name, create_it=False):
    """
    The function checks for the existence of an index on a collection in a MongoDB database and creates
    it if specified.
    
    :param database: The `database` parameter is a string that represents the name of the MongoDB
    database where the collection is located
    :param collection: The `collection` parameter in the `check_index_on_collection` function refers to
    the name of the collection in the MongoDB database on which you want to check for the existence of a
    specific index
    :param index_name: The `index_name` parameter in the `check_index_on_collection` function is a
    string that represents the name of the index that you want to check for on a specific collection in
    a MongoDB database
    :param create_it: The `create_it` parameter in the `check_index_on_collection` function is a boolean
    flag that indicates whether the function should create the specified index if it does not already
    exist in the collection. If `create_it` is set to `True`, the function will attempt to create the
    index if it, defaults to False (optional)
    :return: The function `check_index_on_collection` returns a boolean value indicating whether the
    specified index exists in the collection or not.
    """
    try:
        db, client = connect_to_mongodb(database)
        indexes = db[collection].index_information()
        index_key = f'{collection}_{index_name}'
        have_index = index_key in indexes
        if not have_index and create_it :
            create_index_on_collection(database,collection,index_name)
        close_mongodb_connection(client)
        return have_index
    except Exception as e:
        raise ValueError('Database operation faillure: {e}')