{{ config(
    materialized='incremental',
    unique_key=['CARRIER_TRACKING_NUMBER', 'EVENT_TIME', 'STATUS_TYPE'],
    on_schema_change='fail',
    transient=True
) }}

WITH exploded_data AS (
    select
        -- Clean and standardize tracking information
        trim(upper(r.TRACKINGNUMBER::string)) as CARRIER_TRACKING_NUMBER,
        trim(r.TRANSACTION_ID::string) as TRANSACTION_ID,
        trim(r.TRACKING_ID::string) as TRACKING_ID,
        
        -- Clean status information
        trim(upper(coalesce(s.value:"status":"type"::string, ''))) as STATUS_TYPE,
        trim(coalesce(s.value:"status":"description"::string, '')) as STATUS_DESCRIPTION,
        try_to_timestamp(
            s.value:"date"::string || s.value:"time"::string,
            'YYYYMMDDHH24MISS'
        ) as EVENT_TIME,
        
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"currentStatus":"description"::string, '')) as CURRENT_STATUS,

        -- Sender Information (packageAddress[0])
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"name"::string, '')) as SENDER_NAME,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"addressLine1"::string, '')) as SENDER_ADDRESS1,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"addressLine2"::string, '')) as SENDER_ADDRESS2,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"city"::string, ''))) as SENDER_CITY,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"stateProvince"::string, ''))) as SENDER_STATE,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"postalCode"::string, '')) as SENDER_ZIP,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"countryCode"::string, ''))) as SENDER_COUNTRY,

        -- Recipient Information (packageAddress[1])
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"name"::string, '')) as RECIPIENT_NAME,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"addressLine1"::string, '')) as RECIPIENT_SHIP_ADDRESS1,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"addressLine2"::string, '')) as RECIPIENT_SHIP_ADDRESS2,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"city"::string, ''))) as RECIPIENT_SHIP_CITY,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"stateProvince"::string, ''))) as RECIPIENT_SHIP_STATE,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"postalCode"::string, '')) as RECIPIENT_SHIP_ZIP,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"countryCode"::string, ''))) as RECIPIENT_SHIP_COUNTRY_CODE,

        -- Service & Package
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"service":"description"::string, '')) as SERVICE_DESCRIPTION,
        case 
            when coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"weight":"weight"::float, 0) > 0 
            then try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"weight":"weight"::float
            else null 
        end as WEIGHT,
        trim(upper(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"weight":"unitOfMeasurement"::string, ''))) as WEIGHT_UNIT,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"deliveryInformation":"receivedBy"::string, '')) as RECEIVED_BY,
        1 as ORDER_QUANTITY,
        trim(coalesce(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"referenceNumber"[0]:"number"::string, '')) as FACILITY_ID,

        -- Shipment metadata
        try_to_date(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"pickupDate"::string, 'YYYYMMDD') as SHIP_DATE,
        trim(upper(coalesce(s.value:"location":"address":"city"::string, ''))) as LOCATION,

        -- Timestamps
        r.CREATE_DATE,
        try_to_timestamp(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"deliveryTime":"endTime"::string) as DELIVERY_TIME,
        current_timestamp() as RECORD_LOAD_DATE,
        r.IS_ACTIVE,
        hash(try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"activity") as ACTIVITY_HASH

    from {{ source('sample_database', 'UPS') }} r,
         lateral flatten(input => try_parse_json(r.DATA):"trackResponse":"shipment"[0]:"package"[0]:"activity") as s
    where s.value is not null
    {% if is_incremental() %}
        and r.DATA_LOAD_TIME > (select max(RECORD_LOAD_DATE) from {{ this }})
    {% endif %}
),

ranked_data AS (
    select *,
           row_number() over (
               partition by CARRIER_TRACKING_NUMBER, EVENT_TIME, STATUS_TYPE, ACTIVITY_HASH
               order by RECORD_LOAD_DATE desc
           ) as RN,
           row_number() over (
               partition by CARRIER_TRACKING_NUMBER
               order by EVENT_TIME desc, RECORD_LOAD_DATE desc
           ) as LATEST_RN
    from exploded_data
),

deduped AS (
    select *,
           case when LATEST_RN = 1 then true else false end as IS_ACTIVE_FINAL
    from ranked_data
    where RN = 1
)

select
    CARRIER_TRACKING_NUMBER,
    TRANSACTION_ID,
    TRACKING_ID,
    STATUS_TYPE,
    STATUS_DESCRIPTION,
    EVENT_TIME,
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
    SHIP_DATE,
    LOCATION,
    case 
        when CREATE_DATE is not null 
        then CREATE_DATE::timestamp
        else null 
    end as CREATE_DATE,
    DELIVERY_TIME,
    RECORD_LOAD_DATE,
    IS_ACTIVE_FINAL as IS_ACTIVE
from deduped
order by CARRIER_TRACKING_NUMBER, EVENT_TIME desc
