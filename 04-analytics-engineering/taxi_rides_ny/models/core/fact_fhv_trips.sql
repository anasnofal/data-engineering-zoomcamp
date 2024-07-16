{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select *,
    'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    tripdata.service_type,
    tripdata.dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.pickup_locationid,
    tripdata.dropoff_locationid,
    tripdata.SR_Flag,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
 from tripdata
inner join dim_zones as pickup_zone
on tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripdata.dropoff_locationid = dropoff_zone.locationid
