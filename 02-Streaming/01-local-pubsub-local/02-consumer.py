import csv
import time
from google.cloud import pubsub_v1
import os

# Variáveis
path='/home/logan/projetos/gcp_apache_beam'
aux=f'{path}/Auxiliares'

#serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
serviceAccount = f'{aux}/sturdy-mechanic-322322-2f463c27b249.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription = 'projects/sturdy-mechanic-322322/subscriptions/Udemy_Voos-sub'
subscriber = pubsub_v1.SubscriberClient()

def mostrar_msg(mensagem):
    print('Mensagem: {}'.format(mensagem))
    mensagem.ack()

subscriber.subscribe(subscription,callback=mostrar_msg)

while (True):
    time.sleep(3)