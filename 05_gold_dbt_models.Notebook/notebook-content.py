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
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

"""
╔══════════════════════════════════════════════════════════════════╗
║         HappyBooking — Gold Layer Runner v5                      ║
║         Silver (Lakehouse) → Gold (HappyBooking_Warehouse)       ║
╚══════════════════════════════════════════════════════════════════╝

Workspace : HappyBooking
Warehouse : HappyBooking_Warehouse
Bağlantı  : com.microsoft.spark.fabric → synapsesql
"""

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── KRİTİK IMPORT ─────────────────────────────────────────────────
import com.microsoft.spark.fabric  # noqa — bu satır synapsesql metodunu aktive eder

print("=" * 60)
print("🥇 HappyBooking — Gold Layer Runner v5")
print(f"   Başlangıç: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("   Hedef    : HappyBooking_Warehouse.gold")
print("=" * 60)

WAREHOUSE = "HappyBooking_Warehouse"
SCHEMA    = "gold"

# ─────────────────────────────────────────────────────────────────
# YARDIMCI FONKSİYON
# ─────────────────────────────────────────────────────────────────
def write_to_warehouse(df, table_name, mode="overwrite"):
    full_table = f"{WAREHOUSE}.{SCHEMA}.{table_name}"
    count = df.count()
    print(f"  ⏳ {SCHEMA}.{table_name} yazılıyor... ({count:,} satır)")
    df.write.mode(mode).synapsesql(full_table)
    print(f"  ✅ {SCHEMA}.{table_name} → {count:,} satır yazıldı!")
    return count

# ─────────────────────────────────────────────────────────────────
# 1. SILVER OKU
# ─────────────────────────────────────────────────────────────────
print("\n📖 ADIM 1: silver_bookings okunuyor...")
raw   = spark.read.table("silver_bookings")
total = raw.count()
print(f"  ✅ Toplam kayıt: {total:,}")

# ─────────────────────────────────────────────────────────────────
# 2. fact_booking
# ─────────────────────────────────────────────────────────────────
print("\n🔄 ADIM 2: fact_booking oluşturuluyor...")

fact_booking = raw \
    .withColumn("booking_date",    F.to_date("booking_date")) \
    .withColumn("checkin_date",    F.to_date("checkin_date")) \
    .withColumn("checkout_date",   F.to_date("checkout_date")) \
    .withColumn("stay_nights",     F.col("nights").cast("int")) \
    .withColumn("total_price",     F.col("total_price").cast("double")) \
    .withColumn("room_price",      F.col("room_price").cast("double")) \
    .withColumn("tax_amount",      F.col("tax_amount").cast("double")) \
    .withColumn("service_fee",     F.col("service_fee").cast("double")) \
    .withColumn("paid_amount",     F.col("paid_amount").cast("double")) \
    .withColumn("discount_amount", F.col("discount_amount").cast("double")) \
    .withColumn("booking_year",    F.year("booking_date")) \
    .withColumn("booking_month",   F.month("booking_date")) \
    .withColumn("is_cancelled",    F.col("is_cancelled").cast("boolean")) \
    .select(
        "booking_id", "booking_date", "booking_year", "booking_month",
        "checkin_date", "checkout_date", "stay_nights",
        "booking_status", "booking_channel", "booking_source",
        "is_cancelled", "cancellation_date", "cancellation_reason",
        "customer_id",
        F.col("full_name").alias("customer_name"),
        F.col("country_customer").alias("customer_country"),
        F.col("city_customer").alias("customer_city"),
        F.col("loyalty_level_customer").alias("loyalty_level"),
        "language_preference",
        "hotel_id", "hotel_name",
        F.col("country").alias("hotel_country"),
        F.col("city").alias("hotel_city"),
        "hotel_type", "star_rating",
        "room_type", "rooms_booked", "adults", "children", "infants",
        "num_guests", "special_requests", "promotion_code",
        "total_price", "room_price", "tax_amount", "service_fee",
        "paid_amount", "discount_amount", "payment_status", "payment_method",
        "data_source", "silver_load_at",
    )

print(f"  ✅ fact_booking: {fact_booking.count():,} satır")

# ─────────────────────────────────────────────────────────────────
# 3. dim_hotel
# ─────────────────────────────────────────────────────────────────
print("\n🔄 ADIM 3: dim_hotel oluşturuluyor...")

dim_hotel = raw \
    .select(
        "hotel_id", "hotel_name",
        F.col("country"), F.col("city"),
        "hotel_type", "star_rating", "total_rooms",
        "hotel_facilities", "nearby_attractions",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        "website",
    ) \
    .dropDuplicates(["hotel_id"])

print(f"  ✅ dim_hotel: {dim_hotel.count():,} satır")

# ─────────────────────────────────────────────────────────────────
# 4. dim_city
# ─────────────────────────────────────────────────────────────────
print("\n🔄 ADIM 4: dim_city oluşturuluyor...")

dim_city = raw \
    .filter(F.col("city").isNotNull() & (F.col("city") != "")) \
    .groupBy(
        F.col("city").alias("city_name"),
        F.col("country")
    ) \
    .agg(
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
        F.countDistinct("hotel_id").alias("hotel_count"),
    ) \
    .withColumn("city_id", F.md5(F.concat_ws("_", F.col("city_name"), F.col("country"))))

print(f"  ✅ dim_city: {dim_city.count():,} satır")

# ─────────────────────────────────────────────────────────────────
# 5. dim_customer
# ─────────────────────────────────────────────────────────────────
print("\n🔄 ADIM 5: dim_customer oluşturuluyor...")

dim_customer = raw \
    .select(
        "customer_id", "first_name", "last_name", "full_name",
        "email", "phone", "birth_date", "gender",
        F.col("country_customer").alias("country"),
        F.col("city_customer").alias("city"),
        "address", "postal_code", "language_preference",
        F.col("loyalty_level_customer").alias("loyalty_level"),
        "registration_date", "marketing_consent",
    ) \
    .dropDuplicates(["customer_id"])

print(f"  ✅ dim_customer: {dim_customer.count():,} satır")

# ─────────────────────────────────────────────────────────────────
# 6. kpi_revenue
# ─────────────────────────────────────────────────────────────────
print("\n🔄 ADIM 6: kpi_revenue oluşturuluyor...")

kpi_revenue = fact_booking \
    .groupBy("booking_year", "booking_month", "hotel_city", "hotel_country") \
    .agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum(F.when(~F.col("is_cancelled"), 1).otherwise(0)).alias("confirmed_bookings"),
        F.sum(F.when( F.col("is_cancelled"), 1).otherwise(0)).alias("cancelled_bookings"),
        F.sum("total_price").alias("gross_revenue"),
        F.sum("paid_amount").alias("collected_revenue"),
        F.sum("discount_amount").alias("total_discounts"),
        F.avg("total_price").alias("avg_booking_value"),
        F.sum("stay_nights").alias("total_nights"),
        F.avg("stay_nights").alias("avg_stay_nights"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("hotel_id").alias("active_hotels"),
    ) \
    .withColumn("cancellation_rate",
        F.round(F.col("cancelled_bookings") / F.col("total_bookings") * 100, 2)
    ) \
    .withColumn("revenue_rank",
        F.rank().over(
            Window.partitionBy("booking_year", "booking_month")
                  .orderBy(F.desc("gross_revenue"))
        )
    )

print(f"  ✅ kpi_revenue: {kpi_revenue.count():,} satır")

# ─────────────────────────────────────────────────────────────────
# 7. WAREHOUSE'A YAZ
# ─────────────────────────────────────────────────────────────────
print(f"\n💾 ADIM 7: {WAREHOUSE}.{SCHEMA} yazılıyor...")

results = {}
results["fact_booking"] = write_to_warehouse(fact_booking, "fact_booking")
results["dim_hotel"]    = write_to_warehouse(dim_hotel,    "dim_hotel")
results["dim_city"]     = write_to_warehouse(dim_city,     "dim_city")
results["dim_customer"] = write_to_warehouse(dim_customer, "dim_customer")
results["kpi_revenue"]  = write_to_warehouse(kpi_revenue,  "kpi_revenue")

# ─────────────────────────────────────────────────────────────────
# 8. DOĞRULAMA
# ─────────────────────────────────────────────────────────────────
print(f"\n📊 ADIM 8: Warehouse doğrulaması...")

for table in results.keys():
    try:
        df_check = spark.read.synapsesql(f"{WAREHOUSE}.{SCHEMA}.{table}")
        print(f"  ✅ {SCHEMA}.{table}: {df_check.count():,} satır ✓")
    except Exception as e:
        print(f"  ❌ {SCHEMA}.{table}: {e}")

# ─────────────────────────────────────────────────────────────────
# 9. ÖZET
# ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("📊 GOLD LAYER ÖZET")
print("=" * 60)
print(f"  Kaynak    : silver_bookings ({total:,} satır)")
print(f"  Warehouse : {WAREHOUSE}")
print(f"  Schema    : {SCHEMA}")
print()
print("  Oluşturulan tablolar:")
for tbl, cnt in results.items():
    print(f"    ✅ {SCHEMA}.{tbl:<20} → {cnt:,} satır")
print("=" * 60)
print(f"\n✅ Gold Layer tamamlandı!")
print(f"   Bitiş: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
