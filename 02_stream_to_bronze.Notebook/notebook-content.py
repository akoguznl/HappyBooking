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
║         HappyBooking — Bronze Layer: Stream to Bronze            ║
║         Eventstream (bronze_streaming_bookings) → Doğrulama      ║
╚══════════════════════════════════════════════════════════════════╝

Not: Streaming verisi Fabric Eventstream üzerinden otomatik olarak
bronze_streaming_bookings tablosuna yazılır. Bu notebook:
  1. Streaming tablosunu doğrular
  2. Batch tablosuyla schema uyumunu kontrol eder
  3. Metadata kolonları ekler
  4. Bronze unified tablosunu oluşturur
"""

from pyspark.sql.functions import col, current_timestamp, lit, input_file_name
from pyspark.sql.types import StructType

print("=" * 60)
print("📥 HappyBooking — Stream to Bronze Verification")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# 1. STREAMING TABLOSUNU OKU VE DOĞRULA
# ─────────────────────────────────────────────────────────────────
print("\n📊 ADIM 1: Streaming tablosu kontrol ediliyor...")

try:
    df_stream = spark.read.table("bronze_streaming_bookings")
    stream_count = df_stream.count()
    print(f"  ✅ bronze_streaming_bookings: {stream_count:,} satır")
    print(f"  Kolon sayısı: {len(df_stream.columns)}")
except Exception as e:
    print(f"  ❌ Streaming tablosu okunamadı: {e}")
    print("  Eventstream mapping'ini kontrol edin!")
    raise

# ─────────────────────────────────────────────────────────────────
# 2. BATCH TABLOSUYLA SCHEMA UYUM KONTROLÜ
# ─────────────────────────────────────────────────────────────────
print("\n📊 ADIM 2: Schema uyum kontrolü...")

try:
    df_batch = spark.read.table("bronze_hotel_bookings")
    batch_cols  = set(df_batch.columns)
    stream_cols = set(df_stream.columns)

    only_in_batch  = batch_cols - stream_cols
    only_in_stream = stream_cols - batch_cols
    common_cols    = batch_cols & stream_cols

    print(f"  Ortak kolonlar     : {len(common_cols)}")
    if only_in_batch:
        print(f"  Sadece Batch'te    : {only_in_batch}")
    if only_in_stream:
        print(f"  Sadece Stream'de   : {only_in_stream}")

    if not only_in_batch and not only_in_stream:
        print("  ✅ Schema tam uyumlu!")
    else:
        print("  ⚠️  Schema farklılıkları var — allowMissingColumns=True ile birleştirme yapılacak")

except Exception as e:
    print(f"  ❌ Batch tablosu okunamadı: {e}")
    raise

# ─────────────────────────────────────────────────────────────────
# 3. METADATA KOLONLARI EKLE
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 3: Metadata kolonları ekleniyor...")

# Batch'e source etiketi ekle (yoksa)
if "_source" not in df_batch.columns:
    df_batch = df_batch.withColumn("_source", lit("Batch_CSV"))
if "_ingestion_timestamp" not in df_batch.columns:
    df_batch = df_batch.withColumn("_ingestion_timestamp", current_timestamp())

# Stream'e source etiketi ekle
if "_source" not in df_stream.columns:
    df_stream = df_stream.withColumn("_source", lit("Streaming_Docker"))
if "_ingestion_timestamp" not in df_stream.columns:
    df_stream = df_stream.withColumn("_ingestion_timestamp", current_timestamp())

print("  ✅ _source ve _ingestion_timestamp kolonları eklendi")

# ─────────────────────────────────────────────────────────────────
# 4. UNIFIED BRONZE TABLOSU OLUŞTUR
# ─────────────────────────────────────────────────────────────────
print("\n💾 ADIM 4: Unified Bronze tablosu oluşturuluyor...")

df_unified = df_batch.unionByName(df_stream, allowMissingColumns=True)
unified_count = df_unified.count()

df_unified.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_unified_bookings")

# ─────────────────────────────────────────────────────────────────
# 5. DOĞRULAMA VE ÖZET
# ─────────────────────────────────────────────────────────────────
df_check = spark.read.table("bronze_unified_bookings")
source_dist = df_check.groupBy("_source").count().collect()

print("\n" + "=" * 60)
print("📊 STREAM TO BRONZE ÖZET")
print("=" * 60)
print(f"  Batch satır sayısı   : {df_batch.count():,}")
print(f"  Stream satır sayısı  : {stream_count:,}")
print(f"  Unified toplam       : {unified_count:,}")
print()
print("  Kaynak dağılımı:")
for row in source_dist:
    print(f"    {row['_source']}: {row['count']:,}")
print("=" * 60)
print("\n✅ bronze_unified_bookings tablosu başarıyla oluşturuldu!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
