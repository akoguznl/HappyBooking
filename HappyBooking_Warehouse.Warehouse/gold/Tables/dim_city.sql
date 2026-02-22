CREATE TABLE [gold].[dim_city] (

	[city_name] varchar(max) NULL, 
	[country] varchar(max) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[hotel_count] bigint NOT NULL, 
	[city_id] varchar(max) NOT NULL
);