import datetime
import uuid
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure,AutoReconnect,DuplicateKeyError
from db_helper import close_mongodb_connection,connect_to_mongodb,get_reference_key,next_record_identifier_value
from src.utils import datetime_formater

PROJECTION = {'_id': False, 'id': True}
CRITERIA = {}
SORT = [("created_at", pymongo.DESCENDING)]

def find_one(database:str,collection:str, criteria:dict=CRITERIA, projection:dict=PROJECTION):
    """
    The function `find_one` connects to a MongoDB database, retrieves one document from a specified
    collection based on given criteria and projection, and formats the result using a datetime formatter
    before returning it.
    
    :param database: The `database` parameter in the `find_one` function refers to the name of the
    MongoDB database you want to connect to and perform the find operation on. It is a required
    parameter for the function and should be a string representing the name of the database
    :param collection: The `collection` parameter in the `find_one` function refers to the name of the
    collection within the specified database where the search operation will be performed. It is the
    specific group of documents within a database where data is stored
    :param criteria: The `criteria` parameter in the `find_one` function is a dictionary that specifies
    the conditions that the document must meet in order to be retrieved from the MongoDB collection. It
    is used to filter the documents in the collection based on the specified criteria. The documents
    that match the criteria will be returned by
    :type criteria: dict
    :param projection: The `projection` parameter in the `find_one` function is used to specify which
    fields should be included or excluded in the query result. It is a dictionary where the keys
    represent the fields to be included (with a value of 1) or excluded (with a value of 0) in
    :type projection: dict
    :return: The function `find_one` is returning the result of the MongoDB query after formatting the
    datetime if the data is found, or it returns `None` if no data is found.
    """
    try:
        db,client = connect_to_mongodb(database)
        data = db[collection].find_one(criteria, projection)
        close_mongodb_connection(client)
        return datetime_formater(data) if data else data
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
        raise ValueError(f'Database operation failure {e}')
            
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

 
def insert_one(database:str, collection:str, data, datalist=True):
    """
    The function `insert_one` inserts a document into a MongoDB collection with additional fields like
    creation timestamp and unique identifiers, and returns either a list of documents or a specific
    reference key based on the input parameter.
    
    :param database: The `database` parameter in the `insert_one` function refers to the name of the
    MongoDB database where you want to insert the data
    :param collection: The `collection` parameter in the `insert_one` function refers to the name of the
    collection in the MongoDB database where you want to insert the data. It is a required parameter for
    specifying the target collection for inserting the data
    :param data: The `data` parameter in the `insert_one` function is a dictionary containing the
    information that you want to insert into the specified collection in the MongoDB database. It
    typically includes key-value pairs representing the fields and values of the document you want to
    insert
    :param datalist: The `datalist` parameter in the `insert_one` function is a boolean flag that
    determines whether the function should return a list of data after inserting the new data or just
    the reference key of the inserted data. If `datalist` is set to `True`, the function will return a
    list, defaults to True (optional)
    :return: If `datalist` is `True`, the function will return the result of calling the `finds`
    function with the `database` and `collection` parameters. If `datalist` is `False`, the function
    will return the value associated with the key returned by the `get_reference_key` function from the
    `data` dictionary.
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
            return finds(database, collection)
        else:
            return data[get_reference_key()]
    except (ConnectionFailure, AutoReconnect, OperationFailure,DuplicateKeyError) as e:
        raise ValueError(f'Database operation failure {e}')
    

def aggregate(database:str,collection:str, pipeline:list):
    """
    The function `aggregate` connects to a MongoDB database, performs an aggregation operation on a
    specified collection using a given pipeline, and returns the aggregated data.
    
    :param database: The `database` parameter is a string that represents the name of the MongoDB
    database you want to connect to
    :type database: str
    :param collection: The `collection` parameter in the `aggregate` function refers to the name of the
    collection within the specified MongoDB database where the aggregation operation will be performed.
    It is a string that represents the name of the collection where the data is stored or from where the
    data will be aggregated using the specified pipeline
    :type collection: str
    :param pipeline: The `pipeline` parameter in the `aggregate` function is a list of aggregation
    stages that define the operations to be performed on the data in the specified collection in the
    MongoDB database. These stages can include operations like filtering, grouping, sorting, and
    transforming the data before returning the results. Each stage in
    :type pipeline: list
    :return: The function `aggregate` is returning a list of datetime objects after aggregating data
    from the specified collection in the MongoDB database using the provided pipeline.
    """
    try:
        db ,client = connect_to_mongodb(database)
        datas = [
            datetime(data) for data in db[collection].aggregate(pipeline)
        ]
        close_mongodb_connection(client)
        return datas
    except (ConnectionFailure, AutoReconnect, OperationFailure,DuplicateKeyError) as e:
        raise ValueError(f'Database operation failure {e}')



def count(database:str, collection:str, criteria=CRITERIA):
    """
    The function `count` connects to a MongoDB database, counts the number of documents in a specified
    collection based on given criteria, and returns the count.
    
    :param database: The `database` parameter is a string that represents the name of the MongoDB
    database you want to connect to
    :type database: str
    :param collection: The `collection` parameter in the `count` function refers to the name of the
    collection within the specified MongoDB database where you want to count the documents that match
    the given criteria. It is a string that specifies the collection name in which the count operation
    will be performed
    :type collection: str
    :param criteria: The `criteria` parameter in the `count` function is used to specify the conditions
    that documents must meet to be included in the count operation. It is typically a query document
    that filters the documents in the collection based on certain criteria. If no criteria is provided,
    the function will count all documents in
    :return: The count of documents in the specified collection that match the given criteria is being
    returned by the `count` function.
    """
    try:
        db ,client = connect_to_mongodb(database)
        data = db[collection].count(criteria)
        # close_mongodb_connection(client)
        return data
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
        raise ValueError(f'Database operation failure {e}')
    

def insert_many(database:str,collection:str, datas:list, datalist=False):
    """
    The function `insert_many` inserts a list of data into a MongoDB collection, generates unique
    identifiers, timestamps, and handles exceptions related to database operations.
    
    :param database: The `database` parameter in the `insert_many` function is a string that represents
    the name of the MongoDB database where you want to insert data
    :type database: str
    :param collection: The `collection` parameter in the `insert_many` function refers to the name of
    the collection in the MongoDB database where you want to insert the data. It is a required parameter
    that specifies the target collection for inserting the provided data
    :type collection: str
    :param datas: The `datas` parameter in the `insert_many` function is a list of dictionaries
    containing the data that you want to insert into the specified MongoDB collection. Each dictionary
    represents a single document to be inserted into the collection
    :type datas: list
    :param datalist: The `datalist` parameter in the `insert_many` function is a boolean flag that
    determines whether the function should return the list of records after inserting the data into the
    MongoDB collection. If `datalist` is set to `True`, the function will return the list of records by
    calling the `, defaults to False (optional)
    :return: The function `insert_many` returns a list of record IDs that were inserted into the MongoDB
    collection. If the `datalist` parameter is set to `True`, it also returns the result of the `finds`
    function for the specified database and collection.
    """
    try:
        db ,client = connect_to_mongodb(database)
        datas_= []
        records_ids = []
        # f"{get_reference_key()}s" = []
        # exec(f"{variable_name} = '{[]}'")
        for data in datas :
            date = datetime.datetime.now()
            record_id = str(uuid.uuid4())
            data['created_at'] = date
            data['updated_at'] = date
            data['id'] = next_record_identifier_value(database,collection)
            data[get_reference_key()] = record_id
            datas_.append[data]
            records_ids.append(record_id)
        db[collection].insert_many(datas_)
        close_mongodb_connection(client)
        if datalist:
            return finds(database, collection)
        return records_ids
    except (ConnectionFailure, AutoReconnect, OperationFailure, DuplicateKeyError) as e:
        raise ValueError(f'Database operation failure {e}')


def delete_one(database:str, collection:str, criteria, datalist=False):
    """
    The function `delete_one` deletes a single document from a MongoDB collection based on specified
    criteria and returns True upon successful deletion.
    
    :param database: The `database` parameter in the `delete_one` function is a string that represents
    the name of the MongoDB database where the operation will be performed
    :type database: str
    :param collection: The `collection` parameter in the `delete_one` function refers to the name of the
    collection within the specified database where you want to delete a document based on the provided
    criteria
    :type collection: str
    :param criteria: The `criteria` parameter in the `delete_one` function is used to specify the
    condition that documents in the collection must meet in order to be deleted. It is typically a
    dictionary that contains the fields and values that the documents must match in order to be deleted.
    For example, if you want to
    :param datalist: The `datalist` parameter in the `delete_one` function is a boolean flag that
    indicates whether the function should return the list of remaining documents in the collection after
    deleting one document. If `datalist` is set to `True`, the function will return the list of
    documents. If `datal, defaults to False (optional)
    :return: The function `delete_one` will return `True` if the deletion operation is successful. If
    the `datalist` parameter is set to `True`, it will also return the result of the `finds` function
    for the specified database and collection.
    """
    try:
        db ,client = connect_to_mongodb(database)
        db[collection].delete_one(criteria)
        close_mongodb_connection(client)
        if datalist:
            return finds(database, collection)
        return True
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
       raise ValueError(f'Database operation failure {e}')
   

def delete_many(database, collection, criteria, datalist=True):
    """
    The function `delete_many` deletes multiple documents from a MongoDB collection based on specified
    criteria and returns the remaining documents if `datalist` is True.
    
    :param database: The `database` parameter in the `delete_many` function refers to the name of the
    MongoDB database that you want to connect to and perform operations on. It is used to specify the
    database where the collection is located and where the deletion operation will take place
    :param collection: The `collection` parameter in the `delete_many` function refers to the name of
    the collection within the specified database where the deletion operation will be performed. It is
    the specific group of documents within a database where data is stored
    :param criteria: The `criteria` parameter in the `delete_many` function is used to specify the
    conditions that documents must meet in order to be deleted from the specified collection in the
    database. It is typically a dictionary containing key-value pairs that represent the conditions for
    deletion. Only documents that match all the criteria specified in
    :param datalist: The `datalist` parameter in the `delete_many` function is a boolean flag that
    determines whether the function should return the list of documents that remain in the collection
    after the deletion operation is performed. If `datalist` is set to `True`, the function will return
    the list of remaining documents, defaults to True (optional)
    :return: If the `datalist` parameter is `True`, the function will return the result of the `finds`
    function called with the `database` and `collection` parameters. If the `datalist` parameter is
    `False`, the function will return `True`.
    """
    try:
        db ,client = connect_to_mongodb(database)
        db[collection].delete_many(criteria)
        close_mongodb_connection(client)
        if datalist:
            return finds(database, collection)
        else:
            True
    except (ConnectionFailure, AutoReconnect, OperationFailure) as e:
       raise ValueError(f'Database operation failure {e}')
   

def update_one(database,collection, criteria,update_operation:str, data:dict, datalist=True):
    """
    The function `update_one` updates a document in a MongoDB collection based on specified criteria and
    returns the updated data or a list of documents.
    
    :param database: The `database` parameter in the `update_one` function refers to the name of the
    database in which the collection is located. It is used to specify the database where the update
    operation will be performed
    :param collection: The `collection` parameter in the `update_one` function refers to the name of the
    collection within the specified database where the update operation will be performed. It is the
    specific subset of data within the database that you want to update
    :param criteria: The `criteria` parameter in the `update_one` function is used to specify the
    selection criteria for the document that needs to be updated in the MongoDB collection. It is a
    dictionary that defines the conditions that the document must meet in order to be updated. For
    example, if you want to update a
    :param update_operation: The `update_operation` parameter in the `update_one` function specifies the
    type of update operation to be performed on the specified document in the MongoDB collection. It can
    take values such as `''`, `''`, or `''`, which are MongoDB update operators used to
    :type update_operation: str
    :param data: The `data` parameter in the `update_one` function is a dictionary that contains the
    information you want to update in the MongoDB collection. It should include key-value pairs where
    the keys represent the fields you want to update and the values represent the new values you want to
    set for those fields
    :type data: dict
    :param datalist: The `datalist` parameter in the `update_one` function is a boolean flag that
    determines whether the function should return a list of data after performing the update operation.
    If `datalist` is set to `True`, the function will return the updated list of data from the specified
    `collection`, defaults to True (optional)
    :return: If `datalist` is `True`, the function will return the result of the `finds` function with
    the specified `database` and `collection`. If `datalist` is `False`, it will return `True` after
    closing the MongoDB connection.
    """
    db = client[database]
    try:
        data['updated_at'] = datetime.datetime.now()
        db ,client = connect_to_mongodb(database)
        #'$pull', '$push','$set'
        db[collection].update_one(criteria, {update_operation: data})
        if datalist:
            return finds(database, collection)
        else:
            close_mongodb_connection(client)
            return True
    except (ConnectionFailure, AutoReconnect, OperationFailure,DuplicateKeyError) as e:
       raise ValueError(f'Database operation failure {e}')
