import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'sturdy-mechanic-322322',
    'runner': 'DataflowRunner',
    'region':'us-east1',
    'staging_location':'gs://bkt-estudo-beam/temp',
    'temp_location': 'gs://bkt-estudo-beam/temp',
    'template_location': 'gs://bkt-estudo-beam/template/batch_job_voos_gcs_to_bq',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

# Variáveis
path='/home/logan/projetos/gcp_apache_beam'
aux=f'{path}/Auxiliares'
voos_csv = f'{aux}/voos_sample.csv'

#serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
serviceAccount = f'{aux}/credential-sturdy-mechanic.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
    def process(self,record):
        if int(record[8]) > 0:
            return[record]
        
def criar_dict_nivel1(record):
    dict_ = {}
    dict_['airport'] = record[0]
    dict_['lista'] = record[1]
    return(dict_)

def desaninhar_dict(record):
    def expand(key,value):
        if isinstance(value,dict):
            return [ (key + '_' + k, v) for k, v in desaninhar_dict(value).items() ]
        else:
            return [(key,value)]
    items = [item for k, v in record.items() for item in expand(k,v)]
    return dict(items)

def criar_dict_nivel0(record):
    dict_ = {}
    dict_['airport'] = record['airport']
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
    return(dict_)

table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela='sturdy-mechanic-322322:curso_dataflow_voos.voos_atraso'

Tempo_Atrasos = (
    p1
    | "(Tempo) Importar Dados" >> beam.io.ReadFromText(r"gs://bkt-estudo-beam/entrada/voos_sample.csv", skip_header_lines = 1)
    | "(Tempo) Separar por Virgulas" >> beam.Map(lambda  record: record.split(','))
    | "(Tempo) Pegar voos" >> beam.ParDo(filtro())
    | "(Tempo) Criar pares" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "(Tempo) Somar por Key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    | "(Qtd) Importar Dados" >> beam.io.ReadFromText(r"gs://bkt-estudo-beam/entrada/voos_sample.csv", skip_header_lines = 1)
    | "(Qtd) Separar por Virgulas" >> beam.Map(lambda  record: record.split(','))
    | "(Qtd) Pegar voos" >> beam.ParDo(filtro())
    | "(Qtd) Criar pares" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "(Qtd) Contar por Key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | beam.CoGroupByKey()
    | beam.Map(lambda record: criar_dict_nivel1(record))
    | beam.Map(lambda record: desaninhar_dict(record))
    | beam.Map(lambda record: criar_dict_nivel0(record))
    | beam.io.WriteToBigQuery(
        tabela,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location='gs://bkt-estudo-beam/temp'
    )
)

p1.run()