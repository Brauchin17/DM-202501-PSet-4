# DM-202501-PSet-4
## Levantar Docker Compose
docker-compose up -d
docker-compose ps

## Variables de ambiente
Crea un archivo `.env` con las siguientes variables:
PG_HOST=<host_postgres>
PG_PORT=<puerto_postgres>
PG_DB=<nombre_db>
PG_USER=<usuario_postgres>
PG_PASSWORD=<password_postgres>
PG_SCHEMA_RAW=raw
PG_SCHEMA_ANALYTICS=analytics

## Ingestar Raw

Para ingestar raw es necesario ejecutar el notebook `01_ingesta_parquet_raw.ipynb`, este ingesta todos los datos de taxis desde 2015 a 2025 de los services: `green` y `yellow`, asegurando indempotencia y una política de reintentos además tambiéen ingesta `taxi_zones`, todo esto en el schema `raw`.

## Construir OBT

Para construir obt se va a ejecutar el archivo `built_obt.py`, el cual sigue el siguiente comando: `docker compose run obt-builder --mode by-partition --year-start 2022 --year-end 2025 --services yellow green --run-id run_2022_2025 --overwrite true`, donde se pueden modificar el ano de inicio y final, en caso de usar --mode full se construira `analytics.obt_trips` completo (2015 a 2025), no importa los argumentos en year o services.




