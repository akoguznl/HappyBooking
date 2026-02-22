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
║         HappyBooking — Data Quality: Great Expectations          ║
║         Silver tablosunu test et → GE raporu oluştur            ║
╚══════════════════════════════════════════════════════════════════╝

Proje Gereksinimleri:
  ✅ not_null testleri
  ✅ unique testleri
  ✅ schema validation
  ✅ date format validation
  ✅ GE raporu artifact olarak kaydedilir
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
import json
import os
from datetime import datetime
from pyspark.sql.functions import col

print("=" * 60)
print("🔍 HappyBooking — Great Expectations Data Quality")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# 1. VERİYİ YÜKLEacak
# ─────────────────────────────────────────────────────────────────
print("\n📥 ADIM 1: Silver tablosu yükleniyor...")

df_silver = spark.read.table("silver_bookings")
total_rows = df_silver.count()
print(f"  Toplam satır: {total_rows:,}")

# GE için pandas'a çevir (büyük veri için sample al)
SAMPLE_SIZE = min(100_000, total_rows)
df_pd = df_silver.sample(fraction=SAMPLE_SIZE/total_rows, seed=42).toPandas()
print(f"  GE örnek boyutu: {len(df_pd):,} satır")

# ─────────────────────────────────────────────────────────────────
# 2. GE CONTEXT OLUŞTUR
# ─────────────────────────────────────────────────────────────────
print("\n🔧 ADIM 2: Great Expectations context oluşturuluyor...")

context = gx.get_context()

datasource = context.sources.add_or_update_pandas(name="silver_bookings_source")
data_asset = datasource.add_dataframe_asset(name="silver_bookings_asset")
batch_request = data_asset.build_batch_request(dataframe=df_pd)

# ─────────────────────────────────────────────────────────────────
# 3. EXPECTATION SUITE OLUŞTUR
# ─────────────────────────────────────────────────────────────────
print("\n📋 ADIM 3: Expectation suite tanımlanıyor...")

suite_name = "silver_bookings_suite"
suite = context.add_or_update_expectation_suite(suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

# ── NOT NULL TESTLERİ ──────────────────────────────────────────
print("  NOT NULL testleri tanımlanıyor...")
critical_cols = [
    "booking_id", "checkin_date", "checkout_date",
    "hotel_name", "city", "total_price", "data_source"
]
for c in critical_cols:
    validator.expect_column_values_to_not_be_null(column=c)

# ── UNIQUE TESTLERİ ────────────────────────────────────────────
print("  UNIQUE testleri tanımlanıyor...")
validator.expect_column_values_to_be_unique(column="booking_id")

# ── SCHEMA VALIDATION ─────────────────────────────────────────
print("  Schema testleri tanımlanıyor...")
validator.expect_column_to_exist(column="booking_id")
validator.expect_column_to_exist(column="checkin_date")
validator.expect_column_to_exist(column="checkout_date")
validator.expect_column_to_exist(column="total_price")
validator.expect_column_to_exist(column="silver_load_at")
validator.expect_column_to_exist(column="data_source")
validator.expect_column_to_exist(column="stay_duration_days")

# ── VERİ TİPİ VE DEĞER KONTROLÜ ───────────────────────────────
print("  Değer aralığı testleri tanımlanıyor...")

# Fiyat pozitif olmalı
validator.expect_column_values_to_be_between(
    column="total_price", min_value=0, max_value=50000
)

# Konaklama süresi pozitif olmalı
validator.expect_column_values_to_be_between(
    column="stay_duration_days", min_value=1, max_value=365
)

# data_source sadece bilinen değerleri içermeli
validator.expect_column_values_to_be_in_set(
    column="data_source",
    value_set=["Batch_CSV", "Streaming_Docker"]
)

# room_type standardize edilmiş olmalı
if "room_type" in df_pd.columns:
    validator.expect_column_values_to_be_in_set(
        column="room_type",
        value_set=["SINGLE", "DOUBLE", "SUITE", "TWIN", None],
        mostly=0.90  # %90'ı bu değerlerden biri olmalı
    )

# ── TARIH FORMAT KONTROLÜ ──────────────────────────────────────
print("  Tarih format testleri tanımlanıyor...")
for date_col in ["checkin_date", "checkout_date", "booking_date"]:
    if date_col in df_pd.columns:
        validator.expect_column_values_to_match_strftime_format(
            column=date_col,
            strftime_format="%Y-%m-%d",
            mostly=0.99
        )

# ── NULL ORANI KONTROLÜ ────────────────────────────────────────
# Kritik olmayan kolonlarda max %5 null kabul edilebilir
optional_cols = ["customer_name", "room_type", "booking_status"]
for c in optional_cols:
    if c in df_pd.columns:
        validator.expect_column_values_to_not_be_null(
            column=c, mostly=0.95
        )

# ─────────────────────────────────────────────────────────────────
# 4. TESTLERİ ÇALIŞTIR
# ─────────────────────────────────────────────────────────────────
print("\n🚀 ADIM 4: Testler çalıştırılıyor...")

validator.save_expectation_suite(discard_failed_expectations=False)
checkpoint = context.add_or_update_checkpoint(
    name="silver_checkpoint",
    validator=validator,
)
checkpoint_result = checkpoint.run()

# ─────────────────────────────────────────────────────────────────
# 5. SONUÇLARI PARSE ET VE RAPORLA
# ─────────────────────────────────────────────────────────────────
print("\n📊 ADIM 5: Sonuçlar raporlanıyor...")

results = checkpoint_result.list_validation_results()[0]
stats = results["statistics"]

total_expectations = stats["evaluated_expectations"]
successful         = stats["successful_expectations"]
failed             = stats["unsuccessful_expectations"]
success_pct        = stats["success_percent"]

print("\n" + "=" * 60)
print("📊 GREAT EXPECTATIONS RAPORU")
print("=" * 60)
print(f"  Toplam test       : {total_expectations}")
print(f"  Başarılı          : {successful} ✅")
print(f"  Başarısız         : {failed} {'❌' if failed > 0 else '✅'}")
print(f"  Başarı oranı      : %{success_pct:.1f}")
print()

# Başarısız testleri listele
if failed > 0:
    print("  ❌ Başarısız Testler:")
    for result in results["results"]:
        if not result["success"]:
            exp_type = result["expectation_config"]["expectation_type"]
            col_name = result["expectation_config"]["kwargs"].get("column", "N/A")
            print(f"    - [{col_name}] {exp_type}")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# 6. RAPORU ARTIFACT OLARAK KAYDET
# ─────────────────────────────────────────────────────────────────
print("\n💾 ADIM 6: GE raporu kaydediliyor...")

report = {
    "run_date": datetime.now().isoformat(),
    "table": "silver_bookings",
    "sample_size": len(df_pd),
    "total_rows": total_rows,
    "total_expectations": total_expectations,
    "successful": successful,
    "failed": failed,
    "success_percent": success_pct,
    "overall_success": failed == 0,
    "failed_tests": [
        {
            "column": r["expectation_config"]["kwargs"].get("column", "N/A"),
            "expectation": r["expectation_config"]["expectation_type"]
        }
        for r in results["results"] if not r["success"]
    ]
}

# Fabric Lakehouse'a JSON olarak kaydet
report_json = json.dumps(report, indent=2, ensure_ascii=False)
report_path = "Files/reports/ge_report_silver.json"

dbutils.fs.put(report_path, report_json, overwrite=True)
print(f"  ✅ Rapor kaydedildi: {report_path}")

if failed == 0:
    print("\n🎉 Tüm testler başarıyla geçti!")
else:
    print(f"\n⚠️  {failed} test başarısız — Silver tablosunu gözden geçir!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GE kurulu mu?
import subprocess, sys
try:
    import great_expectations as gx
    print(f"✅ GE kurulu: {gx.__version__}")
except ImportError:
    print("❌ GE bulunamadı, kuruluyor...")
    subprocess.run([sys.executable, "-m", "pip", "install", 
                   "great_expectations", "--quiet"])
    import great_expectations as gx
    print(f"✅ GE kuruldu: {gx.__version__}")

# dbutils var mı? (Fabric'te farklı olabilir)
try:
    dbutils
    print("✅ dbutils mevcut")
except NameError:
    print("❌ dbutils yok — notebookutils kullanacağız")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
