import datetime
import uuid
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure,AutoReconnect,DuplicateKeyError
from db_helper import close_mongodb_connection,connect_to_mongodb,get_reference_key,next_record_identifier_value
from src.utils import datetime_formater

PROJECTION = {'_id': False, 'id': True}
CRITERIA = {}
SORT = [("created_at", pymongo.DESCENDING)]

def finds(database:str, collection:str, projection:dict=PROJECTION, criteria:dict=CRITERIA,sort:list=SORT, limit=0,pager:dict=None):
    """
    This Python function retrieves data from a MongoDB database based on specified criteria, projection,
    sorting, and pagination parameters.
    
    :param database: The `finds` function you provided is used to query a MongoDB database based on the
    specified criteria, projection, sorting, and pagination options. It connects to the MongoDB
    database, performs the query, formats the results, and returns the data
    :param collection: The `collection` parameter in the `finds` function refers to the name of the
    collection in the MongoDB database where you want to perform the find operation. It is a required
    parameter and should be a string specifying the name of the collection you want to query
    :type collection: str
    :param projection: The `projection` parameter in the `finds` function is used to specify which
    fields should be included or excluded in the query results. It is a dictionary where the keys
    represent the fields to include or exclude, and the values indicate whether to include (1) or
    exclude (0) the field
    :type projection: dict
    :param criteria: The `criteria` parameter in the `finds` function is used to specify the conditions
    that must be met for a document to be included in the result set. It is a dictionary that contains
    key-value pairs where the keys represent the fields to check and the values represent the conditions
    that must be satisfied
    :type criteria: dict
    :param sort: The `sort` parameter in the `finds` function is used to specify the order in which the
    documents should be returned from the MongoDB collection. It is a list that defines the fields to
    sort by and the sort order (ascending or descending) for each field
    :type sort: list
    :param limit: The `limit` parameter in the `finds` function specifies the maximum number of
    documents that the MongoDB query will return. If the `limit` parameter is not specified, the default
    value is set to 0, which means that there is no limit on the number of documents returned by the
    query, defaults to 0 (optional)
    :param pager: The `pager` parameter is a dictionary that can contain the keys `skip` and `limit`.
    These keys are used for pagination when querying a MongoDB collection. The `skip` key specifies the
    number of documents to skip before returning results, while the `limit` key specifies the maximum
    number of documents
    :type pager: dict
    :return: a list of data elements from a MongoDB collection based on the specified criteria,
    projection, sorting, and pagination parameters. The data elements are formatted using a datetime
    formatter before being returned.
    """
    db,client = connect_to_mongodb(database)
    keys_to_check = ['skip', 'limit']
    if pager :
        check_keys = all(key in pager for key in keys_to_check)
        if not check_keys :
            raise ValueError('Pager should have skip and limit key vlaue')
        cursor = db[collection].find(criteria, projection).skip(pager["skip"]).limit(pager["limit"]).sort(sort)
    else:
        cursor = db[collection].find(criteria, projection).sort(sort).limit(limit)
    close_mongodb_connection(client)
    datas = [datetime_formater(element) for element in cursor]
    return datas

 
def insert_one(database, collection, data, datalist=True):
    """
    The function `insert_one` inserts a document into a MongoDB collection with additional fields like
    creation and update timestamps, unique identifiers, and returns the inserted data or a list of all
    documents in the collection.
    
    :param database: The `database` parameter in the `insert_one` function refers to the name of the
    MongoDB database where you want to insert the data
    :param collection: The `collection` parameter in the `insert_one` function refers to the name of the
    collection in the MongoDB database where you want to insert the data. It is the specific container
    within a database that holds your documents
    :param data: The `data` parameter in the `insert_one` function is a dictionary containing the
    information that you want to insert into the specified collection in the MongoDB database. This
    dictionary should include the data fields that you want to store in the database document
    :param datalist: The `datalist` parameter in the `insert_one` function is a boolean flag that
    determines whether the function should return a list of all records in the specified collection
    after inserting the new data. If `datalist` is set to `True`, the function will return the list of
    records along with, defaults to True (optional)
    :return: If `datalist` is `True`, the function will return the result of the `finds` function with
    the specified `database` and `collection`, along with a boolean value `True`. If `datalist` is
    `False`, the function will return the value of the key obtained from `get_reference_key()` in the
    inserted `data`.
    """
    try:
        db ,client = connect_to_mongodb(database)
        date = datetime.datetime.now()
        data['created_at'] = date
        data['updated_at'] = date
        data['id'] = next_record_identifier_value(database,collection)
        data[get_reference_key()] = str(uuid.uuid4())
        db[collection].insert_one(data)
        close_mongodb_connection(client)
        if datalist:
            return finds(database, collection), True
        else:
            return data[get_reference_key()]
    except (ConnectionFailure, AutoReconnect, OperationFailure,DuplicateKeyError) as e:
        raise ValueError(f'Database operation failure {e}')
    