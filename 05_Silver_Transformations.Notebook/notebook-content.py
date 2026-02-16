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

from pyspark.sql.functions import col, to_date, current_timestamp, lit

# 1. Bronze tablolarını oku
df_batch = spark.read.table("bronze_hotel_bookings")
df_api = spark.read.table("bronze_currency_api") # Eğer booking verisi içeriyorsa
df_stream = spark.read.table("bronze_streaming_bookings")

# 2. Kaynak belirtmek için 'source' sütunu ekleyelim (Opsiyonel ama iyi bir pratik)
df_batch = df_batch.withColumn("data_source", lit("Batch_CSV"))
df_stream = df_stream.withColumn("data_source", lit("Streaming_Docker"))

# 3. Tabloları alt alta birleştir (Union)
# Not: Sütun isimlerinin tam eşleştiğinden emin oluyoruz
df_combined = df_batch.unionByName(df_stream, allowMissingColumns=True)

# 4. Mükerrer Kayıtları Temizle (Deduplication)
# booking_id üzerinden tekrar edenleri silip en güncelini bırakalım
df_cleaned = df_combined.dropDuplicates(["booking_id"])

# 5. Veri Tiplerini Standartlaştır
df_silver = df_cleaned.withColumn("checkin_date", to_date(col("checkin_date"))) \
                      .withColumn("checkout_date", to_date(col("checkout_date"))) \
                      .withColumn("silver_load_at", current_timestamp())

# 6. Silver Tablosuna Kaydet (Delta Formatında)
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bookings")

print("✅ Silver Tablosu Başarıyla Oluşturuldu!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, current_timestamp, lit

# 1. Kaynak Tabloları Yükle
print("📥 Bronze tabloları okunuyor...")
df_batch = spark.read.table("bronze_hotel_bookings")
df_stream = spark.read.table("bronze_streaming_bookings")

# 2. Tabloları Birleştir (Union)
# allowMissingColumns=True sayesinde sütun sayıları farklı olsa bile hata almayız
df_combined = df_batch.unionByName(df_stream, allowMissingColumns=True)

# 3. VERİ KALİTESİ KONTROLLERİ (Data Quality)
print("🔍 Veri kalitesi kontrolleri başlatılıyor...")

# A. Null Kontrolü: Kritik alanlar boş mu?
null_count = df_combined.filter(col("booking_id").isNull()).count()
if null_count > 0:
    print(f"⚠️ UYARI: {null_count} adet satırda booking_id boş!")

# B. Mantıksal Kontrol: Fiyat negatif olabilir mi?
negative_prices = df_combined.filter(col("total_price") < 0).count()
if negative_prices > 0:
    print(f"❌ HATA: {negative_prices} satırda negatif fiyat tespit edildi!")

# C. Tarih Kontrolü: Giriş tarihi çıkış tarihinden sonra olamaz
# (Not: Sütun isimlerinizin checkin_date ve checkout_date olduğunu varsayıyoruz)
date_error = df_combined.filter(col("checkin_date") > col("checkout_date")).count()
if date_error > 0:
    print(f"❌ HATA: {date_error} satırda giriş/çıkış tarihi çakışması var!")

# 4. TEMİZLİK VE TRANSFORMASYON
# Sadece kalite kontrolünden geçen verileri alıyoruz
df_silver = df_combined.filter(
    (col("booking_id").isNotNull()) & 
    (col("total_price") >= 0)
).dropDuplicates(["booking_id"]) # Mükerrer kayıtları temizle

# Tarih formatlarını düzelt ve yükleme zamanı ekle
df_silver_final = df_silver.withColumn("checkin_date", to_date(col("checkin_date"))) \
                           .withColumn("checkout_date", to_date(col("checkout_date"))) \
                           .withColumn("silver_load_at", current_timestamp())

# 5. Silver Tablosuna Kaydet
print("💾 Silver tablosu güncelleniyor...")
df_silver_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bookings")

print("✅ İşlem başarıyla tamamlandı! 'silver_bookings' tablosu hazır.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
