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

from pyspark.sql.functions import sum, avg, count, round, col

# 1. Temizlenmiş Silver verisini yükle
df_silver = spark.read.table("silver_bookings")

# 2. Otel bazında özet metrikleri (KPI) hesapla
# Hata düzeltmesi: 'is_canceled' yerine 'is_cancelled' kullanıldı
df_gold_hotels = df_silver.groupBy("hotel_name").agg(
    count("booking_id").alias("total_bookings"),
    round(sum("total_price"), 2).alias("total_revenue"),
    round(avg("total_price"), 2).alias("avg_booking_value"),
    sum("is_cancelled").alias("total_cancellations"),
    round((sum("is_cancelled") / count("booking_id")) * 100, 2).alias("cancellation_rate_percentage")
)

# 3. Gold Tablosu olarak kaydet (Overwrite modunda)
df_gold_hotels.write.format("delta").mode("overwrite").saveAsTable("gold_hotel_performance")

print("✅ Gold katmanı başarıyla oluşturuldu: gold_hotel_performance")
display(df_gold_hotels.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
