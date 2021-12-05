#from encryption.encrypt_confidential_data import EncryptData
from webapp.entity_layer.encryption.encrypt_confidential_data import EncryptData

def get_mongo_db_credentials():
    encrypt_data=EncryptData()
    encrypted_user_name="gAAAAABhBrFVOwkIIbPg_qgRhJ9bauegMSI1fku-PFNOrJHZuRPNzvy5O" \
                        "-qJIb5zsRy6bp97azECupKyGaUZhOc5MAiXrFNCeA==".encode('utf-8')
    encrypted_password="gAAAAABhBrFsp6RpwbZGFRsb5sliw35B34-vtcI9tpGd8YYJQX2fxry-3JElbxGJgjxVKnRBGwQAGH35oY1WTI9aGxXC" \
                       "-SKLbA==".encode('utf-8')
    user_name=encrypt_data.decrypt_message(encrypted_user_name).decode('utf-8')
    password=encrypt_data.decrypt_message(encrypted_password).decode('utf-8')
    return {'user_name': user_name, 'password': password}

"""
encrypt_data=EncryptData()
encrypted_send_email=encrypt_data.encrypt_message("machine.learning.application@gmail.com").decode('utf-8')
print("encrypted_send_email:{}".format(encrypted_send_email))

encrypted_send_email_password=encrypt_data.encrypt_message("Aa908990@").decode('utf-8')
print("encrypted_send_email_password:{}".format(encrypted_send_email_password))
"""




