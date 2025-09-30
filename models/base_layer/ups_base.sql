{{ config(
    materialized='table',
    transient=True
) }}

with raw_data as (
    select
        *,
        try_parse_json(DATA) as DATA_JSON
    from {{ source('sample_database', 'UPS') }}
    where IS_ACTIVE = true
),

exploded_data as (
    select
        coalesce(r.TRACKINGNUMBER::string, '') as CARRIER_TRACKING_NUMBER,
        coalesce(r.TRANSACTION_ID::string, '') as TRANSACTION_ID,
        coalesce(r.TRACKING_ID::string, '') as TRACKING_ID,
        coalesce(s.value:"status":"type"::string, '') as STATUS_TYPE,
        coalesce(s.value:"status":"description"::string, '') as STATUS_DESCRIPTION,
        coalesce(s.value:"date"::string, '') as EVENT_TIME_RAW,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"currentStatus":"description"::string, '') as CURRENT_STATUS,
        coalesce(s.value:"location":"address":"name"::string, '') as SENDER_NAME,
        coalesce(s.value:"location":"address":"address1"::string, '') as SENDER_ADDRESS1,
        coalesce(s.value:"location":"address":"address2"::string, '') as SENDER_ADDRESS2,
        coalesce(s.value:"location":"address":"city"::string, '') as SENDER_CITY,
        coalesce(s.value:"location":"address":"stateProvince"::string, '') as SENDER_STATE,
        coalesce(s.value:"location":"address":"zip"::string, '') as SENDER_ZIP,
        coalesce(s.value:"location":"address":"country"::string, '') as SENDER_COUNTRY,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"address1"::string, '') as RECIPIENT_SHIP_ADDRESS1,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"address2"::string, '') as RECIPIENT_SHIP_ADDRESS2,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"city"::string, '') as RECIPIENT_SHIP_CITY,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"stateProvince"::string, '') as RECIPIENT_SHIP_STATE,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"zip"::string, '') as RECIPIENT_SHIP_ZIP,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"countryCode"::string, '') as RECIPIENT_SHIP_COUNTRY_CODE,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"service":"description"::string, '') as SERVICE_DESCRIPTION,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"weight":"weight"::float, 0) as WEIGHT,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"weight":"unitOfMeasurement"::string, '') as WEIGHT_UNIT,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"deliveryInformation":"receivedBy"::string, '') as RECEIVED_BY,
        1 as ORDER_QUANTITY,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"referenceNumber"[0]:"number"::string, '') as FACILITY_ID,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"pickupDate"::string, '') as SHIP_DATE_RAW,
        coalesce(s.value:"location":"address":"city"::string, '') as LOCATION,
        r.CREATE_DATE,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"deliveryTime":"endTime"::string, '') as DELIVERY_TIME_RAW,
        current_timestamp() as RECORD_LOAD_DATE
    from raw_data r,
         lateral flatten(input => r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"activity") as s
    where s.value is not null
),

deduped as (
    select *,
           row_number() over (
               partition by CARRIER_TRACKING_NUMBER
               order by EVENT_TIME_RAW desc
           ) as RN
    from exploded_data
)

select
    CARRIER_TRACKING_NUMBER,
    TRANSACTION_ID,
    TRACKING_ID,
    STATUS_TYPE,
    STATUS_DESCRIPTION,
    NULLIF(EVENT_TIME_RAW,'')::timestamp as EVENT_TIME,
    CURRENT_STATUS,
    SENDER_NAME,
    SENDER_ADDRESS1,
    SENDER_ADDRESS2,
    SENDER_CITY,
    SENDER_STATE,
    SENDER_ZIP,
    SENDER_COUNTRY,
    RECIPIENT_SHIP_ADDRESS1,
    RECIPIENT_SHIP_ADDRESS2,
    RECIPIENT_SHIP_CITY,
    RECIPIENT_SHIP_STATE,
    RECIPIENT_SHIP_ZIP,
    RECIPIENT_SHIP_COUNTRY_CODE,
    SERVICE_DESCRIPTION,
    WEIGHT,
    WEIGHT_UNIT,
    RECEIVED_BY,
    ORDER_QUANTITY,
    FACILITY_ID,
    NULLIF(SHIP_DATE_RAW,'')::date as SHIP_DATE,
    LOCATION,
    CREATE_DATE::timestamp as CREATE_DATE,
    NULLIF(DELIVERY_TIME_RAW,'')::timestamp as DELIVERY_TIME,
    RECORD_LOAD_DATE,
    case when RN = 1 then true else false end as IS_ACTIVE
from deduped
where RN = 1
order by CARRIER_TRACKING_NUMBER, EVENT_TIME desc