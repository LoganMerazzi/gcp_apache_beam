#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

# Variáveis
path='/home/logan/projetos/gcp_apache_beam'
aux=f'{path}/Auxiliares'
voos_csv = f'{aux}/voos_sample.csv'

#serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
serviceAccount = f'{aux}/sturdy-mechanic-322322-2f463c27b249.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

topico = 'projects/sturdy-mechanic-322322/topics/Udemy_Voos'
publisher = pubsub_v1.PublisherClient()

with open(voos_csv, 'rb') as file:
    for row in file:
        print ('Publicando no tópico')
        publisher.publish(topico, row)
        time.sleep(2)