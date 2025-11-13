import os
import argparse
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
 
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA_RAW = os.getenv("PG_SCHEMA_RAW")
PG_SCHEMA_ANALYTICS = os.getenv("PG_SCHEMA_ANALYTICS")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )



parser = argparse.ArgumentParser(description="Build analytics.obt_trips table")
parser.add_argument("--mode", choices=["full", "by-partition"], default="full")
parser.add_argument("--year-start", type=int, required=True)
parser.add_argument("--year-end", type=int, required=True)
parser.add_argument("--services", nargs="+", choices=["yellow", "green"], required=True)
parser.add_argument("--run-id", required=True)
parser.add_argument("--overwrite", choices=["true", "false"], default="false")
args = parser.parse_args()

if args.mode == "full":
    args.overwrite = "true"
    args.year_start = 2015
    args.year_end = 2025
    args.services = ["green", "yellow"]
    

if args.services == "yellow":
    print("Building OBT for yellow service only")
    query = f"""
        {"DROP TABLE IF EXISTS " + PG_SCHEMA_ANALYTICS + ".obt_trips;" if args.overwrite else ""}

        CREATE TABLE IF NOT EXISTS {PG_SCHEMA_ANALYTICS}.obt_trips AS
        WITH yellow AS (
            SELECT
                "RUN_ID",
                "VENDORID",
                "TPEP_PICKUP_DATETIME" AS "PICKUP_DATETIME",
                "TPEP_DROPOFF_DATETIME" AS "DROPOFF_DATETIME",
                "PASSENGER_COUNT",
                "TRIP_DISTANCE",
                "RATECODEID" AS "RATECODEID",
                "STORE_AND_FWD_FLAG",
                "PULOCATIONID",
                "DOLOCATIONID",
                "PAYMENT_TYPE" AS "PAYMENT_TYPE",
                "FARE_AMOUNT",
                "EXTRA",
                "MTA_TAX",
                "TIP_AMOUNT",
                "TOLLS_AMOUNT",
                "IMPROVEMENT_SURCHARGE",
                "TOTAL_AMOUNT",
                "CONGESTION_SURCHARGE",
                "AIRPORT_FEE",
                "CBD_CONGESTION_FEE",
                NULL::integer AS "EHAIL_FEE",
                NULL::double precision AS "TRIP_TYPE",
                "SERVICE_TYPE",
                "SOURCE_YEAR",
                "SOURCE_MONTH",
                "INGESTED_AT_UTC",
                "SOURCE_PATH"
            FROM {PG_SCHEMA_RAW}.yellow_trips
            WHERE "SOURCE_YEAR" BETWEEN %s AND %s
        ),
        -- Estandarización de zonas horarias y normalización
        standardized_trips as (
            select
                *,
                -- Estandarizar zonas horarias
                (("PICKUP_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "PICKUP_DATETIME_EST",
                (("DROPOFF_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "DROPOFF_DATETIME_EST",
                -- Normalizar
                case "VENDORID"
                    when 1 then 'Creative Mobile Technologies, LLC'
                    when 2 then 'Curb Mobility, LLC'
                    when 6 then 'Myle Technologies Inc'
                    when 7 then 'Helix'
                    else 'Not specified'
                end as "VENDORID_DESC",

                case "RATECODEID"
                    when 1 then 'Standard rate'
                    when 2 then 'JFK'
                    when 3 then 'Newark'
                    when 4 then 'Nassau or Westchester'
                    when 5 then 'Negotiated fare'
                    when 6 then 'Group ride'
                    else 'Unknown'
                end as "RATECODE_DESC",

                case "PAYMENT_TYPE"
                    when 0 then 'Flex Fare trip '
                    when 1 then 'Credit card'
                    when 2 then 'Cash'
                    when 3 then 'No charge'
                    when 4 then 'Dispute'
                    when 5 then 'Unknown'
                    when 6 then 'Voided trip'
                    else 'Not specified'
                end as "PAYMENT_TYPE_DESC",

                case "TRIP_TYPE"
                    when 1 then 'Street-hall'
                    when 2 then 'Dispatch'
                    else 'Unknown'
                end as "TRIP_TYPE_DESC",

                case "STORE_AND_FWD_FLAG"
                    when 'Y' then 'Yes'
                    when 'N' then 'No'
                    else 'Unknown'
                end as "STORE_AND_FWD_FLAG_DESC",
                
                -- Duración del viaje en minutos
                EXTRACT(EPOCH FROM ("DROPOFF_DATETIME" - "PICKUP_DATETIME")) / 60 AS "TRIP_DURATION_MINUTES"
            FROM yellow
        ),
        -- Enriquecer con Taxi Zones
        enriched_with_zones as (
            SELECT
                st.*,
                -- Información de pickup location
                pz."Zone" as "PICKUP_ZONE",
                pz."Borough" as "PICKUP_BOROUGH",
                pz."service_zone" as "PICKUP_SERVICE_ZONE",

                -- Información de dropoff location
                dz."Zone" as "DROPOFF_ZONE",
                dz."Borough" as "DROPOFF_BOROUGH",
                dz."service_zone" as "DROPOFF_SERVICE_ZONE"

            from standardized_trips st
            left join RAW.taxi_zones pz 
                on st."PULOCATIONID" = pz."LocationID"
            left join RAW.taxi_zones dz 
                on st."DOLOCATIONID" = dz."LocationID"
        ),
        -- Métricas adicionales y limpieza final
        final as (
            select
                -- Identificadores y metadatos
                "RUN_ID",
                "INGESTED_AT_UTC",
                "SERVICE_TYPE",
                        
                -- Fechas y tiempos
                "SOURCE_YEAR",
                "SOURCE_MONTH",
                "PICKUP_DATETIME_EST" as "PICKUP_DATETIME",
                "DROPOFF_DATETIME_EST" as "DROPOFF_DATETIME",
                "TRIP_DURATION_MINUTES",
                
                -- Datos del viaje

                "VENDORID",
                "VENDORID_DESC",
                "PASSENGER_COUNT",
                "TRIP_DISTANCE",
                "RATECODEID",
                "RATECODE_DESC",
                "STORE_AND_FWD_FLAG_DESC",

                -- Información de ubicación
                "PULOCATIONID",
                "PICKUP_ZONE",
                "PICKUP_BOROUGH",
                "PICKUP_SERVICE_ZONE",
                
                "DOLOCATIONID", 
                "DROPOFF_ZONE",
                "DROPOFF_BOROUGH",
                "DROPOFF_SERVICE_ZONE",
                
                -- Información de pago
                "PAYMENT_TYPE",
                "PAYMENT_TYPE_DESC",
                "FARE_AMOUNT",
                "TIP_AMOUNT",
                "EXTRA",
                "MTA_TAX",
                "TOLLS_AMOUNT",
                "IMPROVEMENT_SURCHARGE",
                "CONGESTION_SURCHARGE",
                "CBD_CONGESTION_FEE",
                "EHAIL_FEE",
                "TOTAL_AMOUNT",
                "AIRPORT_FEE",
                
                -- Campos específicos
                "TRIP_TYPE",
                "TRIP_TYPE_DESC"
            from enriched_with_zones
        )
        SELECT
            -- tiempo
            "PICKUP_DATETIME",
            "DROPOFF_DATETIME",
            CAST("PICKUP_DATETIME" AS date) AS "PICKUP_DATE",
            EXTRACT(HOUR FROM "PICKUP_DATETIME") AS "PICKUP_HOUR",
            CAST("DROPOFF_DATETIME" AS date) AS "DROPOFF_DATE",
            EXTRACT(HOUR FROM "DROPOFF_DATETIME") AS "DROPOFF_HOUR",
            EXTRACT(DOW FROM "PICKUP_DATETIME") AS "DAY_OF_WEEK",
            EXTRACT(MONTH FROM "PICKUP_DATETIME") AS "MONTH",
            EXTRACT(YEAR FROM "PICKUP_DATETIME") AS "YEAR",

            -- ubicacion
            "PULOCATIONID" AS "PU_LOCATION_ID",
            "PICKUP_ZONE" AS "PU_ZONE",
            "PICKUP_BOROUGH" AS "PU_BOROUGH",
            "DOLOCATIONID" AS "DO_LOCATION_ID",
            "DROPOFF_ZONE" AS "DO_ZONE",
            "DROPOFF_BOROUGH" AS "DO_BOROUGH",

            -- servicios y codigos
            "SERVICE_TYPE",
            "VENDORID" AS "VENDOR_ID",
            "VENDORID_DESC" AS "VENDOR_NAME",
            "RATECODEID" AS "RATE_CODE_ID",
            "RATECODE_DESC" AS "RATE_CODE_DESC",
            "PAYMENT_TYPE",
            "PAYMENT_TYPE_DESC",
            "TRIP_TYPE",
            "TRIP_TYPE_DESC",

            -- viaje
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "STORE_AND_FWD_FLAG_DESC" AS "STORE_AND_FWD_FLAG",

            -- tarifas
            "FARE_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TIP_AMOUNT",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "CONGESTION_SURCHARGE",
            "AIRPORT_FEE",
            "TOTAL_AMOUNT",

            -- derivadas
            "TRIP_DURATION_MINUTES" AS "TRIP_DURATION_MIN",
            CASE 
                WHEN "TRIP_DURATION_MINUTES" > 0 
                THEN ("TRIP_DISTANCE" / ("TRIP_DURATION_MINUTES" / 60)) 
                ELSE NULL 
            END AS "AVG_SPEED_MPH",
            CASE 
                WHEN "TOTAL_AMOUNT" > 0 
                THEN ("TIP_AMOUNT" / "TOTAL_AMOUNT") * 100 
                ELSE NULL 
            END AS "TIP_PCT",

            -- lineage
            "RUN_ID",
            "INGESTED_AT_UTC",
            "SERVICE_TYPE" AS "SOURCE_SERVICE",
            "SOURCE_YEAR",
            "SOURCE_MONTH"
        FROM final;
        """
elif args.services == "green":
    print("Building OBT for green service only")
    query = f"""
    {"DROP TABLE IF EXISTS " + PG_SCHEMA_ANALYTICS + ".obt_trips;" if args.overwrite else ""}

    CREATE TABLE IF NOT EXISTS {PG_SCHEMA_ANALYTICS}.obt_trips AS
    WITH green AS (
        SELECT
            "RUN_ID",
            "VENDORID",
            "LPEP_PICKUP_DATETIME" AS "PICKUP_DATETIME",
            "LPEP_DROPOFF_DATETIME" AS "DROPOFF_DATETIME",
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "RATECODEID" AS "RATECODEID",
            "STORE_AND_FWD_FLAG",
            "PULOCATIONID",
            "DOLOCATIONID",
            "PAYMENT_TYPE" AS "PAYMENT_TYPE",
            "FARE_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TIP_AMOUNT",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "TOTAL_AMOUNT",
            "CONGESTION_SURCHARGE",
            NULL::integer AS "AIRPORT_FEE",
            "CBD_CONGESTION_FEE",
            "EHAIL_FEE",
            "TRIP_TYPE",
            "SERVICE_TYPE",
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "INGESTED_AT_UTC",
            "SOURCE_PATH"
        FROM {PG_SCHEMA_RAW}.green_trips
        WHERE "SOURCE_YEAR" BETWEEN %s AND %s
    ),
    -- Estandarización de zonas horarias y normalización
    standardized_trips as (
        select
            *,
            -- Estandarizar zonas horarias
            (("PICKUP_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "PICKUP_DATETIME_EST",
            (("DROPOFF_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "DROPOFF_DATETIME_EST",
            -- Normalizar
            case "VENDORID"
                when 1 then 'Creative Mobile Technologies, LLC'
                when 2 then 'Curb Mobility, LLC'
                when 6 then 'Myle Technologies Inc'
                when 7 then 'Helix'
                else 'Not specified'
            end as "VENDORID_DESC",

            case "RATECODEID"
                when 1 then 'Standard rate'
                when 2 then 'JFK'
                when 3 then 'Newark'
                when 4 then 'Nassau or Westchester'
                when 5 then 'Negotiated fare'
                when 6 then 'Group ride'
                else 'Unknown'
            end as "RATECODE_DESC",

            case "PAYMENT_TYPE"
                when 0 then 'Flex Fare trip '
                when 1 then 'Credit card'
                when 2 then 'Cash'
                when 3 then 'No charge'
                when 4 then 'Dispute'
                when 5 then 'Unknown'
                when 6 then 'Voided trip'
                else 'Not specified'
            end as "PAYMENT_TYPE_DESC",

            case "TRIP_TYPE"
                when 1 then 'Street-hall'
                when 2 then 'Dispatch'
                else 'Unknown'
            end as "TRIP_TYPE_DESC",

            case "STORE_AND_FWD_FLAG"
                when 'Y' then 'Yes'
                when 'N' then 'No'
                else 'Unknown'
            end as "STORE_AND_FWD_FLAG_DESC",
            
            -- Duración del viaje en minutos
            EXTRACT(EPOCH FROM ("DROPOFF_DATETIME" - "PICKUP_DATETIME")) / 60 AS "TRIP_DURATION_MINUTES"
        FROM green
    ),
    -- Enriquecer con Taxi Zones
    enriched_with_zones as (
        SELECT
            st.*,
            -- Información de pickup location
            pz."Zone" as "PICKUP_ZONE",
            pz."Borough" as "PICKUP_BOROUGH",
            pz."service_zone" as "PICKUP_SERVICE_ZONE",

            -- Información de dropoff location
            dz."Zone" as "DROPOFF_ZONE",
            dz."Borough" as "DROPOFF_BOROUGH",
            dz."service_zone" as "DROPOFF_SERVICE_ZONE"

        from standardized_trips st
        left join RAW.taxi_zones pz 
            on st."PULOCATIONID" = pz."LocationID"
        left join RAW.taxi_zones dz 
            on st."DOLOCATIONID" = dz."LocationID"
    ),
    -- Métricas adicionales y limpieza final
    final as (
        select
            -- Identificadores y metadatos
            "RUN_ID",
            "INGESTED_AT_UTC",
            "SERVICE_TYPE",
                    
            -- Fechas y tiempos
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "PICKUP_DATETIME_EST" as "PICKUP_DATETIME",
            "DROPOFF_DATETIME_EST" as "DROPOFF_DATETIME",
            "TRIP_DURATION_MINUTES",
            
            -- Datos del viaje

            "VENDORID",
            "VENDORID_DESC",
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "RATECODEID",
            "RATECODE_DESC",
            "STORE_AND_FWD_FLAG_DESC",

            -- Información de ubicación
            "PULOCATIONID",
            "PICKUP_ZONE",
            "PICKUP_BOROUGH",
            "PICKUP_SERVICE_ZONE",
            
            "DOLOCATIONID", 
            "DROPOFF_ZONE",
            "DROPOFF_BOROUGH",
            "DROPOFF_SERVICE_ZONE",
            
            -- Información de pago
            "PAYMENT_TYPE",
            "PAYMENT_TYPE_DESC",
            "FARE_AMOUNT",
            "TIP_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "CONGESTION_SURCHARGE",
            "CBD_CONGESTION_FEE",
            "EHAIL_FEE",
            "TOTAL_AMOUNT",
            "AIRPORT_FEE",
            
            -- Campos específicos
            "TRIP_TYPE",
            "TRIP_TYPE_DESC"
        from enriched_with_zones
    )
    SELECT
        -- tiempo
        "PICKUP_DATETIME",
        "DROPOFF_DATETIME",
        CAST("PICKUP_DATETIME" AS date) AS "PICKUP_DATE",
        EXTRACT(HOUR FROM "PICKUP_DATETIME") AS "PICKUP_HOUR",
        CAST("DROPOFF_DATETIME" AS date) AS "DROPOFF_DATE",
        EXTRACT(HOUR FROM "DROPOFF_DATETIME") AS "DROPOFF_HOUR",
        EXTRACT(DOW FROM "PICKUP_DATETIME") AS "DAY_OF_WEEK",
        EXTRACT(MONTH FROM "PICKUP_DATETIME") AS "MONTH",
        EXTRACT(YEAR FROM "PICKUP_DATETIME") AS "YEAR",

        -- ubicacion
        "PULOCATIONID" AS "PU_LOCATION_ID",
        "PICKUP_ZONE" AS "PU_ZONE",
        "PICKUP_BOROUGH" AS "PU_BOROUGH",
        "DOLOCATIONID" AS "DO_LOCATION_ID",
        "DROPOFF_ZONE" AS "DO_ZONE",
        "DROPOFF_BOROUGH" AS "DO_BOROUGH",

        -- servicios y codigos
        "SERVICE_TYPE",
        "VENDORID" AS "VENDOR_ID",
        "VENDORID_DESC" AS "VENDOR_NAME",
        "RATECODEID" AS "RATE_CODE_ID",
        "RATECODE_DESC" AS "RATE_CODE_DESC",
        "PAYMENT_TYPE",
        "PAYMENT_TYPE_DESC",
        "TRIP_TYPE",
        "TRIP_TYPE_DESC",

        -- viaje
        "PASSENGER_COUNT",
        "TRIP_DISTANCE",
        "STORE_AND_FWD_FLAG_DESC" AS "STORE_AND_FWD_FLAG",

        -- tarifas
        "FARE_AMOUNT",
        "EXTRA",
        "MTA_TAX",
        "TIP_AMOUNT",
        "TOLLS_AMOUNT",
        "IMPROVEMENT_SURCHARGE",
        "CONGESTION_SURCHARGE",
        "AIRPORT_FEE",
        "TOTAL_AMOUNT",

        -- derivadas
        "TRIP_DURATION_MINUTES" AS "TRIP_DURATION_MIN",
        CASE 
            WHEN "TRIP_DURATION_MINUTES" > 0 
            THEN ("TRIP_DISTANCE" / ("TRIP_DURATION_MINUTES" / 60)) 
            ELSE NULL 
        END AS "AVG_SPEED_MPH",
        CASE 
            WHEN "TOTAL_AMOUNT" > 0 
            THEN ("TIP_AMOUNT" / "TOTAL_AMOUNT") * 100 
            ELSE NULL 
        END AS "TIP_PCT",

        -- lineage
        "RUN_ID",
        "INGESTED_AT_UTC",
        "SERVICE_TYPE" AS "SOURCE_SERVICE",
        "SOURCE_YEAR",
        "SOURCE_MONTH"
    FROM final;
    """


else:
    print("Building OBT for both services: yellow and green")
    query = f"""
    {"DROP TABLE IF EXISTS " + PG_SCHEMA_ANALYTICS + ".obt_trips;" if args.overwrite else ""}

    CREATE TABLE IF NOT EXISTS {PG_SCHEMA_ANALYTICS}.obt_trips AS
    WITH yellow AS (
        SELECT
            "RUN_ID",
            "VENDORID",
            "TPEP_PICKUP_DATETIME" AS "PICKUP_DATETIME",
            "TPEP_DROPOFF_DATETIME" AS "DROPOFF_DATETIME",
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "RATECODEID" AS "RATECODEID",
            "STORE_AND_FWD_FLAG",
            "PULOCATIONID",
            "DOLOCATIONID",
            "PAYMENT_TYPE" AS "PAYMENT_TYPE",
            "FARE_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TIP_AMOUNT",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "TOTAL_AMOUNT",
            "CONGESTION_SURCHARGE",
            "AIRPORT_FEE",
            "CBD_CONGESTION_FEE",
            NULL::integer AS "EHAIL_FEE",
            NULL::double precision AS "TRIP_TYPE",
            "SERVICE_TYPE",
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "INGESTED_AT_UTC",
            "SOURCE_PATH"
        FROM {PG_SCHEMA_RAW}.yellow_trips
        WHERE "SOURCE_YEAR" BETWEEN %s AND %s
    ),
    green AS (
        SELECT
            "RUN_ID",
            "VENDORID",
            "LPEP_PICKUP_DATETIME" AS "PICKUP_DATETIME",
            "LPEP_DROPOFF_DATETIME" AS "DROPOFF_DATETIME",
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "RATECODEID" AS "RATECODEID",
            "STORE_AND_FWD_FLAG",
            "PULOCATIONID",
            "DOLOCATIONID",
            "PAYMENT_TYPE" AS "PAYMENT_TYPE",
            "FARE_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TIP_AMOUNT",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "TOTAL_AMOUNT",
            "CONGESTION_SURCHARGE",
            NULL::integer AS "AIRPORT_FEE",
            "CBD_CONGESTION_FEE",
            "EHAIL_FEE",
            "TRIP_TYPE",
            "SERVICE_TYPE",
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "INGESTED_AT_UTC",
            "SOURCE_PATH"
        FROM {PG_SCHEMA_RAW}.green_trips
        WHERE "SOURCE_YEAR" BETWEEN %s AND %s
    ),
    unioned_trips AS (
        SELECT * FROM yellow
        UNION ALL
        SELECT * FROM green
    ),
    -- Estandarización de zonas horarias y normalización
    standardized_trips as (
        select
            *,
            -- Estandarizar zonas horarias
            (("PICKUP_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "PICKUP_DATETIME_EST",
            (("DROPOFF_DATETIME" AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York') AS "DROPOFF_DATETIME_EST",
            -- Normalizar
            case "VENDORID"
                when 1 then 'Creative Mobile Technologies, LLC'
                when 2 then 'Curb Mobility, LLC'
                when 6 then 'Myle Technologies Inc'
                when 7 then 'Helix'
                else 'Not specified'
            end as "VENDORID_DESC",

            case "RATECODEID"
                when 1 then 'Standard rate'
                when 2 then 'JFK'
                when 3 then 'Newark'
                when 4 then 'Nassau or Westchester'
                when 5 then 'Negotiated fare'
                when 6 then 'Group ride'
                else 'Unknown'
            end as "RATECODE_DESC",

            case "PAYMENT_TYPE"
                when 0 then 'Flex Fare trip '
                when 1 then 'Credit card'
                when 2 then 'Cash'
                when 3 then 'No charge'
                when 4 then 'Dispute'
                when 5 then 'Unknown'
                when 6 then 'Voided trip'
                else 'Not specified'
            end as "PAYMENT_TYPE_DESC",

            case "TRIP_TYPE"
                when 1 then 'Street-hall'
                when 2 then 'Dispatch'
                else 'Unknown'
            end as "TRIP_TYPE_DESC",

            case "STORE_AND_FWD_FLAG"
                when 'Y' then 'Yes'
                when 'N' then 'No'
                else 'Unknown'
            end as "STORE_AND_FWD_FLAG_DESC",
            
            -- Duración del viaje en minutos
            EXTRACT(EPOCH FROM ("DROPOFF_DATETIME" - "PICKUP_DATETIME")) / 60 AS "TRIP_DURATION_MINUTES"
        FROM unioned_trips
    ),
    -- Enriquecer con Taxi Zones
    enriched_with_zones as (
        SELECT
            st.*,
            -- Información de pickup location
            pz."Zone" as "PICKUP_ZONE",
            pz."Borough" as "PICKUP_BOROUGH",
            pz."service_zone" as "PICKUP_SERVICE_ZONE",

            -- Información de dropoff location
            dz."Zone" as "DROPOFF_ZONE",
            dz."Borough" as "DROPOFF_BOROUGH",
            dz."service_zone" as "DROPOFF_SERVICE_ZONE"

        from standardized_trips st
        left join RAW.taxi_zones pz 
            on st."PULOCATIONID" = pz."LocationID"
        left join RAW.taxi_zones dz 
            on st."DOLOCATIONID" = dz."LocationID"
    ),
    -- Métricas adicionales y limpieza final
    final as (
        select
            -- Identificadores y metadatos
            "RUN_ID",
            "INGESTED_AT_UTC",
            "SERVICE_TYPE",
                    
            -- Fechas y tiempos
            "SOURCE_YEAR",
            "SOURCE_MONTH",
            "PICKUP_DATETIME_EST" as "PICKUP_DATETIME",
            "DROPOFF_DATETIME_EST" as "DROPOFF_DATETIME",
            "TRIP_DURATION_MINUTES",
            
            -- Datos del viaje

            "VENDORID",
            "VENDORID_DESC",
            "PASSENGER_COUNT",
            "TRIP_DISTANCE",
            "RATECODEID",
            "RATECODE_DESC",
            "STORE_AND_FWD_FLAG_DESC",

            -- Información de ubicación
            "PULOCATIONID",
            "PICKUP_ZONE",
            "PICKUP_BOROUGH",
            "PICKUP_SERVICE_ZONE",
            
            "DOLOCATIONID", 
            "DROPOFF_ZONE",
            "DROPOFF_BOROUGH",
            "DROPOFF_SERVICE_ZONE",
            
            -- Información de pago
            "PAYMENT_TYPE",
            "PAYMENT_TYPE_DESC",
            "FARE_AMOUNT",
            "TIP_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "CONGESTION_SURCHARGE",
            "CBD_CONGESTION_FEE",
            "EHAIL_FEE",
            "TOTAL_AMOUNT",
            "AIRPORT_FEE",
            
            -- Campos específicos
            "TRIP_TYPE",
            "TRIP_TYPE_DESC"
        from enriched_with_zones
    )
    SELECT
        -- tiempo
        "PICKUP_DATETIME",
        "DROPOFF_DATETIME",
        CAST("PICKUP_DATETIME" AS date) AS "PICKUP_DATE",
        EXTRACT(HOUR FROM "PICKUP_DATETIME") AS "PICKUP_HOUR",
        CAST("DROPOFF_DATETIME" AS date) AS "DROPOFF_DATE",
        EXTRACT(HOUR FROM "DROPOFF_DATETIME") AS "DROPOFF_HOUR",
        EXTRACT(DOW FROM "PICKUP_DATETIME") AS "DAY_OF_WEEK",
        EXTRACT(MONTH FROM "PICKUP_DATETIME") AS "MONTH",
        EXTRACT(YEAR FROM "PICKUP_DATETIME") AS "YEAR",

        -- ubicacion
        "PULOCATIONID" AS "PU_LOCATION_ID",
        "PICKUP_ZONE" AS "PU_ZONE",
        "PICKUP_BOROUGH" AS "PU_BOROUGH",
        "DOLOCATIONID" AS "DO_LOCATION_ID",
        "DROPOFF_ZONE" AS "DO_ZONE",
        "DROPOFF_BOROUGH" AS "DO_BOROUGH",

        -- servicios y codigos
        "SERVICE_TYPE",
        "VENDORID" AS "VENDOR_ID",
        "VENDORID_DESC" AS "VENDOR_NAME",
        "RATECODEID" AS "RATE_CODE_ID",
        "RATECODE_DESC" AS "RATE_CODE_DESC",
        "PAYMENT_TYPE",
        "PAYMENT_TYPE_DESC",
        "TRIP_TYPE",
        "TRIP_TYPE_DESC",

        -- viaje
        "PASSENGER_COUNT",
        "TRIP_DISTANCE",
        "STORE_AND_FWD_FLAG_DESC" AS "STORE_AND_FWD_FLAG",

        -- tarifas
        "FARE_AMOUNT",
        "EXTRA",
        "MTA_TAX",
        "TIP_AMOUNT",
        "TOLLS_AMOUNT",
        "IMPROVEMENT_SURCHARGE",
        "CONGESTION_SURCHARGE",
        "AIRPORT_FEE",
        "TOTAL_AMOUNT",

        -- derivadas
        "TRIP_DURATION_MINUTES" AS "TRIP_DURATION_MIN",
        CASE 
            WHEN "TRIP_DURATION_MINUTES" > 0 
            THEN ("TRIP_DISTANCE" / ("TRIP_DURATION_MINUTES" / 60)) 
            ELSE NULL 
        END AS "AVG_SPEED_MPH",
        CASE 
            WHEN "TOTAL_AMOUNT" > 0 
            THEN ("TIP_AMOUNT" / "TOTAL_AMOUNT") * 100 
            ELSE NULL 
        END AS "TIP_PCT",

        -- lineage
        "RUN_ID",
        "INGESTED_AT_UTC",
        "SERVICE_TYPE" AS "SOURCE_SERVICE",
        "SOURCE_YEAR",
        "SOURCE_MONTH"
    FROM final;
    """

print(f"Ejecutando creación de OBT_TRIPS ({args.mode})...")
print(f"Años: {args.year_start}–{args.year_end}, Servicios: {args.services}, RunID: {args.run_id}")

try:
    conn = get_connection()
    cur = conn.cursor()
    #cur.execute("CREATE SCHEMA IF NOT EXISTS " + PG_SCHEMA_ANALYTICS + ";")
    cur.execute(query, 
                (args.year_start, args.year_end,
                 args.year_start, args.year_end))
    conn.commit()
    print("Tabla creada correctamente en schema:", PG_SCHEMA_ANALYTICS)

except Exception as e:
    print("Error durante la ejecución:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()



