# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eafed1ea-663b-4dce-bb74-1e8420473b01",
# META       "default_lakehouse_name": "HappyBooking_Lakehouse",
# META       "default_lakehouse_workspace_id": "4469d25c-bf26-4abf-a3d9-12b6ba355076",
# META       "known_lakehouses": [
# META         {
# META           "id": "eafed1ea-663b-4dce-bb74-1e8420473b01"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp

# --- 1. Hava Durumu Verisi (Amsterdam) ---
# Enlem: 52.3676, Boylam: 4.9041
weather_url = "https://api.open-meteo.com/v1/forecast?latitude=52.3676&longitude=4.9041&daily=temperature_2m_max,temperature_2m_min,rain_sum&timezone=Europe%2FBerlin"

try:
    weather_response = requests.get(weather_url).json()
    daily_data = weather_response['daily']
    df_weather_pd = pd.DataFrame(daily_data)
    
    # Spark DataFrame'e çevir ve metadata ekle
    spark_weather = spark.createDataFrame(df_weather_pd) \
        .withColumn("city", lit("Amsterdam")) \
        .withColumn("ingestion_timestamp", current_timestamp())
    
    spark_weather.write.format("delta").mode("overwrite").saveAsTable("bronze_weather_api")
    print("✅ Amsterdam Hava Durumu verisi kaydedildi.")
except Exception as e:
    print(f"❌ Hava durumu çekilirken hata oluştu: {e}")

# --- 2. Döviz Kuru Verisi (EUR Bazlı) ---
currency_url = "https://api.exchangerate-api.com/v4/latest/EUR"

try:
    currency_response = requests.get(currency_url).json()
    # Sadece ihtiyacımız olan kurları (USD, GBP, TRY vb.) veya tüm listeyi alabiliriz
    rates = currency_response['rates']
    df_currency_pd = pd.DataFrame([rates])
    
    spark_currency = spark.createDataFrame(df_currency_pd) \
        .withColumn("base_currency", lit("EUR")) \
        .withColumn("extracted_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    spark_currency.write.format("delta").mode("overwrite").saveAsTable("bronze_currency_api")
    print("✅ Döviz kuru verisi kaydedildi.")
except Exception as e:
    print(f"❌ Döviz kuru çekilirken hata oluştu: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
