import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'sturdy-mechanic-322322',
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://bkt-estudo-beam/temp',
    'temp_location': 'gs://bkt-estudo-beam/temp',
    'template_location': 'gs://bkt-estudo-beam/template/batch_job_df_gcs_voos'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]

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
    | beam.io.WriteToText(r"gs://bkt-estudo-beam/saida/voos_atrasados_qtd.csv")
)

p1.run()