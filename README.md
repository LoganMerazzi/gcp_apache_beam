# Google Dataflow e Apache Beam no GCP

## Funcionamento
```mermaid
graph TD
SDK[Java SDK]
SDK2[Python SDK]
SDK3[Whatever SDK]

SDK & SDK2 & SDK3 --> Beam --> Spark & Dataflow & Flink

```

## Modelos de uso

```mermaid
graph LR

L1[Local]
L2[Local]

L1 --> Beam --> L2

```

```mermaid
graph LR

L1[Local]
L2[GCS]

L1 --> Beam --> L2

```

```mermaid
graph LR

L1[GCS]
L2[GCS]

User --> L1 --> DataFlow --> L2

```

## Para instalar o Apache Beam localmente

- Upgrade do pip
    - *python -m pip install --upgrade pip*
- instalar o Apache Beam e o Apache Beam com os componente do GCP
    - pip install apache-beam
    - pip install apache-beam[gcp]
    É necessário garantir que esteja instalado o pacote pandas, pois o beam faz uso na importação dos arquivos (aparentemente)    
    - pip install pandas
