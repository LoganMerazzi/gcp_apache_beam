import apache_beam as beam
import os

serviceAccount = r'D:\Projetos\GCP_Dataflow_Beam\credential-sturdy-mechanic.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline()

class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return[record]

Tempo_Atrasos = (
p1
    | "(Tempo): Importar os dados" >> beam.io.ReadFromText(r"D:\Projetos\GCP_Dataflow_Beam\Auxiliares\voos_sample.csv", skip_header_lines=1)
    | "(Tempo): Separar por vírgulas" >> beam.Map(lambda record: record.split(','))
    | "(Tempo): Pegar vôos de Los Angeles" >> beam.ParDo(filtro())
    | "(Tempo): Criar o par" >> beam.Map(lambda record: (record[4], int(record[9])))
    | "(Tempo): Somar por key" >> beam.CombinePerKey(sum)
)


Qtd_Atrasos = (
p1
    | "(Qtd): Importar os dados" >> beam.io.ReadFromText(r"D:\Projetos\GCP_Dataflow_Beam\Auxiliares\voos_sample.csv", skip_header_lines=1)
    | "(Qtd): Separar por vírgulas" >> beam.Map(lambda record: record.split(','))
    | "(Qtd): Pegar vôos de Los Angeles" >> beam.ParDo(filtro())
    | "(Qtd): Criar o par" >> beam.Map(lambda record: (record[4], int(record[9])))
    | "(Qtd): contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
    | "Envio para GCP" >> beam.io.WriteToText(r"gs://bkt-estudo-beam/Voos_atrasos_qtd.csv")
)

p1.run()