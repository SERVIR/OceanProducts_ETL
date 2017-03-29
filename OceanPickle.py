import pickle
import csv
import binascii
mydict = {'base_Folder': 'D:\SERVIR\Scripts\OceanProductsETL\\','template_Folder': 'E:\TEMP\OP_Templates\\', 'extract_Folder': 'E:\Temp\OP_Extract\\', 'AWS_ACCESS_KEY_ID': 'YOUR_ID', 'AWS_SECRET_ACCESS_KEY': 'YOUR_KEY', 'bucket': 'bucket.servirglobal.net', 'bucket_path': '/regions/mesoamerica/data/eodata/redtide/v1/2016/', 'ftp_server': 'samoa.gsfc.nasa.gov', 'ftp_path': r'/subscriptions/MODISA/XM/francisco.delgadoolivares/2626/', 'logFileDir': 'D:\Logs\ETL_Logs\OP'}
output = open('config.pkl', 'wb')
pickle.dump(mydict, output)
output.close()