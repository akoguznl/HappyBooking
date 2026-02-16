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

# 1. CSV dosyasının yolunu tanımla
# Not: Dosya yolunu 'Files/raw_data/hotel_raw_batch.csv' olarak güncelleyebilirsin
file_path = "Files/raw_data/hotel_raw_batch.csv" 

# 2. Spark ile CSV'yi oku
# Bronze katmanında veri tipini Spark'ın tahmin etmesine (inferSchema) izin veriyoruz
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .load(file_path)

# 3. Kolon isimlerindeki boşlukları ve özel karakterleri temizleyelim 
# (Delta tabloları kolon isimlerinde boşluk sevmez)
from pyspark.sql.functions import col
for column in df_bronze.columns:
    new_column = column.replace(" ", "_").replace("(", "").replace(")", "").lower()
    df_bronze = df_bronze.withColumnRenamed(column, new_column)

# 4. Bronze Tablosu olarak kaydet
# Bronze katmanında veriyi olduğu gibi (Raw) saklıyoruz
table_name = "bronze_hotel_bookings"
df_bronze.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Başarılı: {table_name} tablosu oluşturuldu.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM HappyBooking_Lakehouse.dbo.bronze_hotel_bookings LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
