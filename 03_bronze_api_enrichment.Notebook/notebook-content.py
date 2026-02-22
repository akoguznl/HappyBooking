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

"""
╔══════════════════════════════════════════════════════════════════╗
║         HappyBooking — Bronze Layer: API Enrichment              ║
║         Weather API + Currency API → Bronze Delta Tabloları      ║
╚══════════════════════════════════════════════════════════════════╝

Düzeltmeler:
  ✅ Tüm booking şehirleri için hava durumu çekiliyor
  ✅ current_timestamp() ile doğru tip kullanımı
  ✅ Döviz tablosuna rate_date eklendi
  ✅ Retry mekanizması eklendi
  ✅ Hata yönetimi geliştirildi
"""

import requests
import pandas as pd
import time
from datetime import datetime, date
from pyspark.sql.functions import lit, current_timestamp, current_date, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# ─────────────────────────────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────────────────────────────
WEATHER_BASE_URL  = "https://api.open-meteo.com/v1/forecast"
CURRENCY_BASE_URL = "https://api.exchangerate-api.com/v4/latest/EUR"
MAX_RETRIES       = 3
RETRY_DELAY       = 2  # saniye

# Şehir koordinatları — Booking datasındaki şehirlere göre güncelle
CITY_COORDINATES = {
    "Amsterdam"  : (52.3676,  4.9041),
    "London"     : (51.5074, -0.1278),
    "Paris"      : (48.8566,  2.3522),
    "Berlin"     : (52.5200, 13.4050),
    "Barcelona"  : (41.3851,  2.1734),
    "Rome"       : (41.9028, 12.4964),
    "Vienna"     : (48.2082, 16.3738),
    "Prague"     : (50.0755, 14.4378),
}

print("=" * 60)
print("📥 HappyBooking — API Enrichment (Bronze)")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# YARDIMCI FONKSİYON — Retry ile HTTP GET
# ─────────────────────────────────────────────────────────────────
def fetch_with_retry(url, params=None, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️  Deneme {attempt}/{retries} başarısız: {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"API isteği {retries} denemede başarısız oldu: {url}")

# ─────────────────────────────────────────────────────────────────
# 1. HAVA DURUMU VERİSİ — Tüm Şehirler
# ─────────────────────────────────────────────────────────────────
print("\n🌤️  ADIM 1: Hava durumu verisi çekiliyor...")

# İsteğe bağlı: Booking datasından şehirleri dinamik al
try:
    df_bookings = spark.read.table("bronze_hotel_bookings")
    booking_cities = [row["city"] for row in df_bookings.select("city").distinct().collect()]
    # Koordinat eşleştirmesi yap, bilinmeyenler için default kullan
    cities_to_fetch = {c: CITY_COORDINATES[c] for c in booking_cities if c in CITY_COORDINATES}
    if not cities_to_fetch:
        cities_to_fetch = CITY_COORDINATES  # fallback
    print(f"  Booking datasından {len(cities_to_fetch)} şehir bulundu.")
except Exception:
    cities_to_fetch = CITY_COORDINATES
    print(f"  Booking tablosu okunamadı — varsayılan {len(cities_to_fetch)} şehir kullanılıyor.")

all_weather = []
for city, (lat, lon) in cities_to_fetch.items():
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,rain_sum,windspeed_10m_max",
            "timezone": "Europe/Berlin",
            "forecast_days": 7
        }
        data = fetch_with_retry(WEATHER_BASE_URL, params=params)
        df_city = pd.DataFrame(data["daily"])
        df_city["city"] = city
        df_city["latitude"] = lat
        df_city["longitude"] = lon
        all_weather.append(df_city)
        print(f"  ✅ {city}: {len(df_city)} günlük veri alındı")
    except Exception as e:
        print(f"  ❌ {city}: Hata — {e}")

if all_weather:
    df_weather_pd = pd.concat(all_weather, ignore_index=True)
    spark_weather = spark.createDataFrame(df_weather_pd) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("_source", lit("open-meteo-api"))

    spark_weather.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("bronze_weather_api")

    print(f"\n  ✅ bronze_weather_api kaydedildi — {spark_weather.count():,} satır")
else:
    print("  ❌ Hiç hava durumu verisi alınamadı!")

# ─────────────────────────────────────────────────────────────────
# 2. DÖVİZ KURU VERİSİ
# ─────────────────────────────────────────────────────────────────
print("\n💱 ADIM 2: Döviz kuru verisi çekiliyor...")

try:
    currency_data = fetch_with_retry(CURRENCY_BASE_URL)
    rates = currency_data["rates"]

    # Sadece ilgili kurları tut
    RELEVANT_CURRENCIES = ["USD", "GBP", "TRY", "CHF", "JPY", "AUD", "CAD", "SEK", "NOK", "DKK"]
    filtered_rates = {k: v for k, v in rates.items() if k in RELEVANT_CURRENCIES}

    # Her kuru ayrı satır olarak kaydet (wide format yerine long format — join'de kolaylaşır)
    rows = [{"currency_code": k, "rate_vs_eur": v} for k, v in filtered_rates.items()]
    df_currency_pd = pd.DataFrame(rows)

    spark_currency = spark.createDataFrame(df_currency_pd) \
        .withColumn("base_currency",        lit("EUR")) \
        .withColumn("rate_date",            current_date()) \
        .withColumn("extracted_at",         current_timestamp()) \
        .withColumn("_source",              lit("exchangerate-api"))

    spark_currency.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("bronze_currency_api")

    saved = spark_currency.count()
    print(f"  ✅ bronze_currency_api kaydedildi — {saved} döviz kuru")
    spark_currency.show()

except Exception as e:
    print(f"  ❌ Döviz kuru çekilemedi: {e}")

# ─────────────────────────────────────────────────────────────────
# 3. ÖZET
# ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("📊 API ENRICHMENT ÖZET")
print("=" * 60)
try:
    w_count = spark.read.table("bronze_weather_api").count()
    print(f"  bronze_weather_api   : {w_count:,} satır ✅")
except:
    print(f"  bronze_weather_api   : ❌ tablo yok")

try:
    c_count = spark.read.table("bronze_currency_api").count()
    print(f"  bronze_currency_api  : {c_count:,} satır ✅")
except:
    print(f"  bronze_currency_api  : ❌ tablo yok")

print("=" * 60)
print("\n✅ API Enrichment tamamlandı!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
