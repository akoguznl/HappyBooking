CREATE TABLE [gold].[kpi_revenue] (

	[booking_year] int NULL, 
	[booking_month] int NULL, 
	[hotel_city] varchar(max) NULL, 
	[hotel_country] varchar(max) NULL, 
	[total_bookings] bigint NOT NULL, 
	[confirmed_bookings] bigint NULL, 
	[cancelled_bookings] bigint NULL, 
	[gross_revenue] float NULL, 
	[collected_revenue] float NULL, 
	[total_discounts] float NULL, 
	[avg_booking_value] float NULL, 
	[total_nights] bigint NULL, 
	[avg_stay_nights] float NULL, 
	[unique_customers] bigint NOT NULL, 
	[active_hotels] bigint NOT NULL, 
	[cancellation_rate] float NULL, 
	[revenue_rank] int NOT NULL
);