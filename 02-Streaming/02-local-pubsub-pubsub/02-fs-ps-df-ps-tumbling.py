import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

pipeline_options = {
    'project': 'sturdy-mechanic-322322',
    'runner': 'DataflowRunner',
    'region':'us-east1',
    'staging_location':'gs://bkt-estudo-beam/temp',
    'temp_location': 'gs://bkt-estudo-beam/temp',
    'template_location': 'gs://bkt-estudo-beam/template/streaming-tumbling',
    'save_main_session': True,
    'streaming': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

# Variáveis
path='/home/logan/projetos/gcp_apache_beam'
aux=f'{path}/Auxiliares'
voos_csv = f'{aux}/voos_sample.csv'

#serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
serviceAccount = f'{aux}/sturdy-mechanic-322322-2f463c27b249.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription = 'projects/sturdy-mechanic-322322/subscriptions/UdemyVoos-sub'
saida = 'projects/sturdy-mechanic-322322/topics/UdemyVoos-output'

class separar_linhas(beam.DoFn):
    def process(self, record):
        return[record.decode("utf-8").split(',')]

class filtro(beam.DoFn):
    def process(self,record):
        if int(record[8]) > 0:
            return[record]
        
#def criar_dict_nivel1(record):
#    dict_ = {}
#    dict_['airport'] = record[0]
#    dict_['lista'] = record[1]
#    return(dict_)
#
#def desaninhar_dict(record):
#    def expand(key,value):
#        if isinstance(value,dict):
#            return [ (key + '_' + k, v) for k, v in desaninhar_dict(value).items() ]
#        else:
#            return [(key,value)]
#    items = [item for k, v in record.items() for item in expand(k,v)]
#    return dict(items)

#def criar_dict_nivel0(record):
#    dict_ = {}
#    dict_['airport'] = record['airport']
#    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
#    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
#    return(dict_)

#table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
#tabela='sturdy-mechanic-322322:curso_dataflow_voos.voos_atraso'

pcollection_entrada = (
    p1 | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=subscription)
)

Tempo_Atrasos = (
    pcollection_entrada
    | "(Tempo) Separar por Virgulas" >> beam.ParDo(separar_linhas())
    | "(Tempo) Pegar voos" >> beam.ParDo(filtro())
    | "(Tempo) Criar pares" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "(Tempo) Window" >> beam.WindowInto(window.FixedWindows(5))
    | "(Tempo) Somar por Key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    pcollection_entrada
    | "(Qtd) Separar por Virgulas" >> beam.ParDo(separar_linhas())
    | "(Qtd) Pegar voos" >> beam.ParDo(filtro())
    | "(Qtd) Criar pares" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "(Qtd) Window" >> beam.WindowInto(window.FixedWindows(5))
    | "(Qtd) Contar por Key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "Join atrasos e Qtd" >> beam.CoGroupByKey()
    | "Converting to Bytes String" >> beam.Map(lambda row: (''.join(str(row)).encode('utf-8')) )
    | "Escrever no tópico" >> beam.io.WriteToPubSub(saida)
)

result = p1.run()
result.wait_until_finish()