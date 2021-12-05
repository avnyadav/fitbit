from webapp.data_access_layer.mongo_db.mongo_db_atlas import MongoDBOperation
from webapp.entity_layer.encryption.encrypt_confidential_data import EncryptData
mgdb = MongoDBOperation()

database_name = "Credentials"

def get_azure_event_hub_namespace_connection_str():
    database_name = "Credentials"
    collection_name = "event_hub_name_space"
    data = mgdb.get_record(database_name, collection_name, {})
    if data is None:
        return False
    credentials=data['connection_str']
    return credentials


def save_azure_event_hub_namespace_connection_str(connection_str):
    database_name = "Credentials"
    collection_name="event_hub_name_space"
    record={'connection_str':connection_str}
    data = mgdb.insert_record_in_collection(database_name, collection_name, record)
    if data>0:
        print("aws_event_hub_namespace_connection_str has been saved")
    else:
        print("Error occured")




def get_aws_credentials():
    try:
        database_name = "Credentials"
        collection_name = "aws"
        data = mgdb.get_record(database_name, collection_name, {'name': 'aws_access_key'})
        credentials={'access_key_id' :data['Access Key ID'],
        'secret_access_key' : data['Secret Access Key']}
        return credentials
    except Exception as e:
        raise e

def create_aws_credentials(name,access_key_id,secret_access_key,description="aws credentials"):
    try:
        data = {
            "name": name,
            "Access Key ID":access_key_id,
            "Secret Access Key": secret_access_key,
            "description": description
        }
        database_name = "Credentials"
        collection_name = "aws"
        result=mgdb.insert_record_in_collection(database_name, collection_name, data)
    except Exception as e:
        raise e



def get_azure_blob_storage_connection_str():
    database_name = "Credentials"
    collection_name = "azure_blob_storage_connection_str"
    data = mgdb.get_record(database_name, collection_name, {})
    if data is None:
        return False
    credentials = data['connection_str']
    return credentials
    #return "DefaultEndpointsProtocol=https;AccountName=storageaccountrgai197de;AccountKey=o56dC8V4h6GmP1bin7a9NO0e6pUWTJrf/lzO3ogX1vFOTnodTCKbZp5VsRHfCSSHDLF8XqMKP0wDVi82eiaT7Q==;EndpointSuffix=core.windows.net"

def save_azure_blob_storage_connection_str(connection_str):
    database_name = "Credentials"
    collection_name = "azure_blob_storage_connection_str"
    record = {'connection_str': connection_str}
    data = mgdb.insert_record_in_collection(database_name, collection_name, record)
    if data > 0:
        print("azure_blob_storage_connection_str has been saved")
    else:
        print("Error occured")


def get_google_cloud_storage_credentials():
    data = mgdb.get_record('Credentials', 'gcp', {})
    return data

def get_azure_input_file_storage_connection_str():
    database_name = "Credentials"
    collection_name = "azure_input_file_storage_connection_str"
    data = mgdb.get_record(database_name, collection_name, {})
    if data is None:
        return False
    credentials = data['connection_str']
    return credentials

    #return "DefaultEndpointsProtocol=https;AccountName=inputfilestorage;AccountKey=u2jUt3DLgsPiZO8S7kZ1yk4B7FJ6gCtdTeckaige1AXTnyPA1zby1w0n6HUi/2cdOAoo5E1jXrEeofyI6UDtFw==;EndpointSuffix=core.windows.net"

def save_azure_input_file_storage_connection_str(connection_str):
    database_name = "Credentials"
    collection_name = "azure_input_file_storage_connection_str"
    record = {'connection_str': connection_str}
    data = mgdb.insert_record_in_collection(database_name, collection_name, record)
    if data > 0:
        print("azure_input_file_storage_connection_str has been saved")
    else:
        print("Error occured")


def get_watcher_checkpoint_storage_account_connection_str():
    database_name = "Credentials"
    collection_name = "watcher_checkpoint_storage_account_connection_str"
    data = mgdb.get_record(database_name, collection_name, {})
    if data is None:
        return False
    credentials = data['connection_str']
    return credentials

    #return "DefaultEndpointsProtocol=https;AccountName=storageaccountrgai1a757;AccountKey=j4TYtRYwIDyUNL4lKXXIfYgCftrTbJPhPHC8i/j66XSf6s81ruCaQqDnjh5WDMjrqqc4hulvZ8wxcYQb8KfukA==;EndpointSuffix=core.windows.net"
#print(get_aws_event_hub_namespace_connection_str())
#save_aws_event_hub_namespace_connection_str("Endpoint=sb://avnish327030.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BwK0zozJe+Oz46F5BgpHUtq5zkzqMKCzPdc2kOg2WW8=")

def save_watcher_checkpoint_storage_account_connection_str(connection_str):
    database_name = "Credentials"
    collection_name = "watcher_checkpoint_storage_account_connection_str"
    record = {'connection_str': connection_str}
    data = mgdb.insert_record_in_collection(database_name, collection_name, record)
    if data > 0:
        print("watcher_checkpoint_storage_account_connection_str has been saved")
    else:
        print("Error occured")

def get_sender_email_id_credentials():
    encrypt_data=EncryptData()
    encrypted_email_address="gAAAAABhBCyldMc5eLzz1kVl8bBIBuOjxZgbgb9K7rFs7nMR-jWB1VO_xUrnA6j6RukxwVZJHMbYjFLt-A4xDm7" \
                            "-729zD4hH_yuiPXJPPWHr4gewfs0Z4_o= "
    encrypted_password="gAAAAABhIzu6IoJzFjX1Fv4UvzoLmOjot7P5J5fFpwnsqZBrvmNyfPwonF-kurj6oe4dbm9_S_HgpGrD9DjCn6DPYdSy3I4dEScXAL7xvun0SO48c0wpdT4="
    result={
        'email_address':encrypt_data.decrypt_message(encrypted_email_address.encode('utf-8')).decode('utf-8'),
        'passkey':encrypt_data.decrypt_message(encrypted_password.encode('utf-8')).decode('utf-8'),
    }
    return result

def get_receiver_email_id_credentials():
    collection_name="email_data"
    """
    record={'receiver_email_addresses':'yadav.tara.avnish@gmail.com;anishyadav7045075175@gmail.com;yadav.tara.ashish@gmail.com;'}

    result=mgdb.get_record(database_name=database_name,collection_name=collection_name,query=record)
    if result is None:
        print("Hi")
        mgdb.insert_record_in_collection(db_name=database_name,collection_name=collection_name,record=record)
        """
    result = mgdb.get_record(database_name=database_name, collection_name=collection_name, query={})
    result={'receiver_email_addresses':'sunny@ineuron.ai,Avnish@ineuron.ai'}
    return result.get('receiver_email_addresses',None)

def save_session_id():
    import uuid
    mgdb.insert_record_in_collection(db_name='session',collection_name='secretKey',record=
                                     {'secret-key':uuid.uuid4().__str__()})

