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
║         HappyBooking — Silver Layer Transformation               ║
║         Bronze (Batch + Stream) → Silver (Cleaned & Enriched)    ║
╚══════════════════════════════════════════════════════════════════╝

Proje Gereksinimleri Karşılanan Adımlar:
  ✅ NULL temizleme (kritik kolonlar)
  ✅ Duplicate temizleme
  ✅ Veri tipi düzeltme
  ✅ Tarih format standardizasyonu
  ✅ Metin standardizasyonu (trim + upper/lower)
  ✅ Kategorik standardizasyon (room_type, hotel_type)
  ✅ Outlier analizi (fiyat, gece sayısı)
  ✅ Tarih mantık kontrolü (checkin < checkout)
  ✅ data_source kolonu
  ✅ Audit log (kaç satır silindi, neden)
"""

from pyspark.sql.functions import (
    col, to_date, current_timestamp, lit,
    trim, upper, lower, when, regexp_replace,
    datediff, abs as spark_abs, round as spark_round,
    count, isnan, coalesce
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime

# ─────────────────────────────────────────────────────────────────
# 0. YARDIMCI FONKSİYON — Audit Log
# ─────────────────────────────────────────────────────────────────
audit_log = []

def log_step(step_name, df_before, df_after, note=""):
    dropped = df_before.count() - df_after.count()
    audit_log.append({
        "step": step_name,
        "rows_before": df_before.count(),
        "rows_after": df_after.count(),
        "rows_dropped": dropped,
        "note": note
    })
    if dropped > 0:
        print(f"  ⚠️  [{step_name}] {dropped} satır silindi. {note}")
    else:
        print(f"  ✅  [{step_name}] Tüm satırlar geçti.")

# ─────────────────────────────────────────────────────────────────
# 1. BRONZE TABLOLARINI OKU
# ─────────────────────────────────────────────────────────────────
print("=" * 60)
print("📥 ADIM 1: Bronze tabloları okunuyor...")
print("=" * 60)

df_batch  = spark.read.table("bronze_hotel_bookings")
df_stream = spark.read.table("bronze_streaming_bookings")

print(f"  Batch satır sayısı  : {df_batch.count()}")
print(f"  Stream satır sayısı : {df_stream.count()}")

# ─────────────────────────────────────────────────────────────────
# 2. KAYNAK ETİKETİ EKLE + BİRLEŞTİR
# ─────────────────────────────────────────────────────────────────
print("\n📥 ADIM 2: Tablolar birleştiriliyor...")

df_batch  = df_batch.withColumn("data_source", lit("Batch_CSV"))
df_stream = df_stream.withColumn("data_source", lit("Streaming_Docker"))

# allowMissingColumns=True: Eksik kolonlar NULL ile doldurulur
df_combined = df_batch.unionByName(df_stream, allowMissingColumns=True)
total_raw = df_combined.count()
print(f"  Birleşik satır sayısı: {total_raw}")

# ─────────────────────────────────────────────────────────────────
# 3. NULL TEMİZLEME — Kritik Kolonlar
# ─────────────────────────────────────────────────────────────────
print("\n🔍 ADIM 3: NULL temizleme başlatılıyor...")

# Null sayımı — her kolon için raporla
critical_cols = ["booking_id", "checkin_date", "checkout_date",
                 "hotel_name", "city", "total_price"]

print("  Null sayımı (kritik kolonlar):")
for c in critical_cols:
    null_cnt = df_combined.filter(col(c).isNull()).count()
    if null_cnt > 0:
        print(f"    ❌ {c}: {null_cnt} null değer")
    else:
        print(f"    ✓  {c}: temiz")

df_no_nulls = df_combined.dropna(subset=critical_cols)
log_step("NULL Temizleme", df_combined, df_no_nulls,
         f"Kritik kolonlar: {critical_cols}")

# ─────────────────────────────────────────────────────────────────
# 4. DUPLICATE TEMİZLEME
# ─────────────────────────────────────────────────────────────────
print("\n🔍 ADIM 4: Duplicate temizleme...")

df_dedup = df_no_nulls.dropDuplicates(["booking_id"])
log_step("Duplicate Temizleme", df_no_nulls, df_dedup,
         "booking_id bazında tekrar eden kayıtlar silindi")

# ─────────────────────────────────────────────────────────────────
# 5. VERİ TİPİ DÖNÜŞÜMÜ
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 5: Veri tipleri dönüştürülüyor...")

# num_guests sendeki adults, children ve infants kolonlarından hesaplanarak oluşturuldu
df_typed = df_dedup \
    .withColumn("booking_date",  to_date(col("booking_date"))) \
    .withColumn("checkin_date",  to_date(col("checkin_date"))) \
    .withColumn("checkout_date", to_date(col("checkout_date"))) \
    .withColumn("total_price",   col("total_price").cast(DoubleType())) \
    .withColumn("num_guests", (
        coalesce(col("adults").cast(IntegerType()), lit(0)) + 
        coalesce(col("children").cast(IntegerType()), lit(0)) + 
        coalesce(col("infants").cast(IntegerType()), lit(0))
    ))

# Tarih dönüşümü sonrası oluşan NULL'ları da temizle
df_typed = df_typed.dropna(subset=["booking_date", "checkin_date", "checkout_date"])
log_step("Tarih Dönüşüm Sonrası NULL", df_dedup, df_typed,
         "Hatalı tarih formatı içeren satırlar (örn: '0000-00-00') temizlendi")

print("  ✅ Tarih ve sayısal tipler dönüştürüldü.")

# ─────────────────────────────────────────────────────────────────
# 6. METİN STANDARDİZASYONU
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 6: Metin standardizasyonu yapılıyor...")

# customer_name yerine sendeki mevcut olan full_name kolonu kullanıldı
df_text_clean = df_typed \
    .withColumn("hotel_name", trim(upper(col("hotel_name")))) \
    .withColumn("city",       trim(upper(col("city")))) \
    .withColumn("country",    trim(upper(col("country")))) \
    .withColumn("full_name",
                trim(regexp_replace(col("full_name"), r"\s+", " ")))

print("  ✅ hotel_name, city, country → UPPER + TRIM")
print("  ✅ full_name → ekstra boşluklar temizlendi")

# ─────────────────────────────────────────────────────────────────
# 7. KATEGORİK STANDARDİZASYON
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 7: Kategorik değerler standardize ediliyor...")

# Room type standardizasyonu
df_categorical = df_text_clean.withColumn("room_type",
    when(lower(col("room_type")).isin("single", "sngl", "1"), "SINGLE")
    .when(lower(col("room_type")).isin("double", "dbl", "2"),  "DOUBLE")
    .when(lower(col("room_type")).isin("suite", "ste"),         "SUITE")
    .when(lower(col("room_type")).isin("twin"),                "TWIN")
    .otherwise(upper(col("room_type")))
)

# Rezervasyon durumu standardizasyonu (varsa)
if "booking_status" in df_text_clean.columns:
    df_categorical = df_categorical.withColumn("booking_status",
        when(lower(col("booking_status")).isin("confirmed", "confirm", "cnf"), "CONFIRMED")
        .when(lower(col("booking_status")).isin("cancelled", "canceled", "cxl"), "CANCELLED")
        .when(lower(col("booking_status")).isin("pending", "pnd"), "PENDING")
        .otherwise(upper(col("booking_status")))
    )

print("  ✅ room_type standardize edildi → SINGLE / DOUBLE / SUITE / TWIN")
print("  ✅ booking_status standardize edildi → CONFIRMED / CANCELLED / PENDING")

# ─────────────────────────────────────────────────────────────────
# 8. MANTIK KONTROLLERİ
# ─────────────────────────────────────────────────────────────────
print("\n🔍 ADIM 8: Mantık kontrolleri uygulanıyor...")

# 8A. Checkin tarihi checkout'tan önce olmalı
date_errors = df_categorical.filter(
    datediff(col("checkout_date"), col("checkin_date")) <= 0
).count()
if date_errors > 0:
    print(f"  ❌ {date_errors} satırda checkin >= checkout — bu satırlar siliniyor!")

df_logic = df_categorical.filter(
    datediff(col("checkout_date"), col("checkin_date")) > 0
)
log_step("Tarih Mantık Kontrolü", df_categorical, df_logic,
         "checkin >= checkout olan satırlar silindi")

# 8B. Konaklama süresi hesapla
df_logic = df_logic.withColumn(
    "stay_duration_days",
    datediff(col("checkout_date"), col("checkin_date"))
)

# ─────────────────────────────────────────────────────────────────
# 9. OUTLİER ANALİZİ
# ─────────────────────────────────────────────────────────────────
print("\n🔍 ADIM 9: Outlier analizi yapılıyor...")

# Fiyat istatistikleri
price_stats = df_logic.select(
    col("total_price")
).summary("min", "25%", "75%", "max").collect()

print("  Fiyat dağılımı:")
for row in price_stats:
    print(f"    {row['summary']}: {row['total_price']}")

# Negatif veya sıfır fiyat
neg_price = df_logic.filter(col("total_price") <= 0).count()
if neg_price > 0:
    print(f"  ❌ {neg_price} satırda fiyat <= 0")

# Aşırı yüksek fiyat (örn: 99999 gibi placeholder değerler)
extreme_price = df_logic.filter(col("total_price") > 50000).count()
if extreme_price > 0:
    print(f"  ⚠️  {extreme_price} satırda fiyat > 50,000 (kontrol et)")

# Outlier'ları filtrele
df_no_outliers = df_logic.filter(
    (col("total_price") > 0) &
    (col("total_price") < 50000) &
    (col("stay_duration_days") <= 365)  # 1 yıldan fazla konaklama — şüpheli
)
log_step("Outlier Temizleme", df_logic, df_no_outliers,
         "Fiyat <= 0, fiyat > 50000, veya konaklama > 365 gün")

# ─────────────────────────────────────────────────────────────────
# 10. METADATA EKLE
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 10: Metadata kolonları ekleniyor...")

df_silver = df_no_outliers \
    .withColumn("silver_load_at", current_timestamp()) \
    .withColumn("silver_version", lit("1.0"))

# ─────────────────────────────────────────────────────────────────
# 11. SILVER TABLOSUNA KAYDET
# ─────────────────────────────────────────────────────────────────
print("\n💾 ADIM 11: Silver tablosuna yazılıyor...")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_bookings")

# ─────────────────────────────────────────────────────────────────
# 12. ÖZET RAPOR
# ─────────────────────────────────────────────────────────────────
final_count = df_silver.count()
total_dropped = total_raw - final_count
retention_rate = round((final_count / total_raw) * 100, 2)

print("\n" + "=" * 60)
print("📊 SILVER TRANSFORMATION ÖZET RAPORU")
print("=" * 60)
print(f"  Ham veri (Bronze)     : {total_raw:,} satır")
print(f"  Silver veri           : {final_count:,} satır")
print(f"  Toplam silinen        : {total_dropped:,} satır")
print(f"  Veri tutma oranı      : %{retention_rate}")
print()
print("  Adım bazlı detay:")
for entry in audit_log:
    print(f"    [{entry['step']}] "
          f"{entry['rows_before']:,} → {entry['rows_after']:,} "
          f"(-{entry['rows_dropped']:,})")
print("=" * 60)
print()

# Schema kontrolü
print("📋 Silver Schema:")
df_silver.printSchema()

print("\n✅ Silver tablosu başarıyla oluşturuldu: 'silver_bookings'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
