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
║         HappyBooking — Bronze Layer: Batch Ingest                ║
║         hotel_raw_batch.csv → bronze_hotel_bookings (Delta)      ║
╚══════════════════════════════════════════════════════════════════╝
"""

from pyspark.sql.functions import (
    col, current_timestamp, lit, input_file_name
)

# ─────────────────────────────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────────────────────────────
FILE_PATH  = "Files/raw_data/hotel_raw_batch.csv"
TABLE_NAME = "bronze_hotel_bookings"
SOURCE_TAG = "Batch_CSV"

print("=" * 60)
print("📥 HappyBooking — Bronze Batch Ingest")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# 1. CSV'Yİ OKU
# ─────────────────────────────────────────────────────────────────
print(f"\n📂 Dosya okunuyor: {FILE_PATH}")

df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(FILE_PATH)

raw_count = df_bronze.count()
print(f"  Ham satır sayısı  : {raw_count:,}")
print(f"  Kolon sayısı      : {len(df_bronze.columns)}")

# ─────────────────────────────────────────────────────────────────
# 2. KOLON İSİMLERİNİ TEMİZLE
# ─────────────────────────────────────────────────────────────────
print("\n🔧 Kolon isimleri temizleniyor...")
rename_map = {}
for col_name in df_bronze.columns:
    clean = (col_name
             .strip()
             .lower()
             .replace(" ", "_")
             .replace("(", "")
             .replace(")", "")
             .replace("-", "_")
             .replace("/", "_"))
    if clean != col_name:
        rename_map[col_name] = clean
        print(f"  '{col_name}' → '{clean}'")

for old, new in rename_map.items():
    df_bronze = df_bronze.withColumnRenamed(old, new)

# ─────────────────────────────────────────────────────────────────
# 3. BRONZE METADATA KOLONLARI EKLE
# ─────────────────────────────────────────────────────────────────
print("\n🔧 Metadata kolonları ekleniyor...")

df_bronze = df_bronze \
    .withColumn("_source",               lit(SOURCE_TAG)) \
    .withColumn("_ingestion_timestamp",  current_timestamp()) \
    .withColumn("_source_file",          input_file_name())

print("  ✅ _source, _ingestion_timestamp, _source_file eklendi")

# ─────────────────────────────────────────────────────────────────
# 4. SCHEMA KONTROLÜ
# ─────────────────────────────────────────────────────────────────
print("\n📋 Schema:")
df_bronze.printSchema()

# ─────────────────────────────────────────────────────────────────
# 5. BRONZE TABLOSUNA KAYDET (Delta)
# ─────────────────────────────────────────────────────────────────
print(f"\n💾 Delta tablosuna yazılıyor: {TABLE_NAME}")

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_NAME)

# ─────────────────────────────────────────────────────────────────
# 6. DOĞRULAMA
# ─────────────────────────────────────────────────────────────────
df_check = spark.read.table(TABLE_NAME)
saved_count = df_check.count()

print("\n" + "=" * 60)
print("📊 BRONZE BATCH INGEST ÖZET")
print("=" * 60)
print(f"  Kaynak dosya      : {FILE_PATH}")
print(f"  Hedef tablo       : {TABLE_NAME}")
print(f"  Okunan satır      : {raw_count:,}")
print(f"  Kaydedilen satır  : {saved_count:,}")
print(f"  Durum             : {'✅ Eşleşiyor' if raw_count == saved_count else '❌ UYUMSUZ!'}")
print("=" * 60)
print(f"\n✅ '{TABLE_NAME}' tablosu başarıyla oluşturuldu!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
