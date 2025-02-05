# Diseño de datos para consumo de API spaceflight
Este desarrollo implementa una **arquitectura de procesamiento de datos** en AWS, permitiendo la extracción, transformación y carga de datos desde **API Spaceflight News API** hasta **Amazon Redshift Serverless**.

## 📌 Flujo de datos 

1. **Extracción desde API**
2. **Almacenamiento en S3 (Zona raw)**
3. **Transformación con AWS Glue (Zona curada)**
4. **Carga en Redshift Serverless**
5. **Orquestacion DAG con Airflow**


