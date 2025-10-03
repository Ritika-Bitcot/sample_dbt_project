{{ config(
    materialized='table',
    transient=True
) }}

with raw_data as (
    select
        _LINE,
        _FIVETRAN_SYNCED,
        TRACKING_ID,
        CREATE_DATE,
        DATA,
        DATA_LOAD_TIME,
        DATA_UPDATE_TIME,
        IS_ACTIVE,
        TRANSACTION_ID,
        ULTIMATE_TRANSACTION_ID,
        TRACKINGNUMBER,
        try_parse_json(DATA) as DATA_JSON
    from {{ source('sample_database', 'UPS') }}
),

exploded_data as (
    select
        -- Clean and standardize tracking information
        trim(upper(r.TRACKINGNUMBER::string)) as CARRIER_TRACKING_NUMBER,
        trim(r.TRANSACTION_ID::string) as TRANSACTION_ID,
        trim(r.TRACKING_ID::string) as TRACKING_ID,
        
        -- Clean status information
        trim(upper(coalesce(s.value:"status":"type"::string, ''))) as STATUS_TYPE,
        trim(coalesce(s.value:"status":"description"::string, '')) as STATUS_DESCRIPTION,
        case 
            when s.value:"date" is not null and s.value:"time" is not null 
            then substr(s.value:"date"::string, 1, 4) || '-' || 
                 substr(s.value:"date"::string, 5, 2) || '-' || 
                 substr(s.value:"date"::string, 7, 2) || ' ' || 
                 case 
                     when length(s.value:"time"::string) = 6 
                     then substr(s.value:"time"::string, 1, 2) || ':' || 
                          substr(s.value:"time"::string, 3, 2) || ':' || 
                          substr(s.value:"time"::string, 5, 2)
                     else s.value:"time"::string
                 end
            else s.value:"date"::string
        end as EVENT_TIME_RAW,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"currentStatus":"description"::string, '')) as CURRENT_STATUS,
        
        -- Clean sender information (from packageAddress where type = 'ORIGIN')
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"name"::string, '')) as SENDER_NAME,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"addressLine1"::string, '')) as SENDER_ADDRESS1,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"addressLine2"::string, '')) as SENDER_ADDRESS2,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"city"::string, ''))) as SENDER_CITY,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"stateProvince"::string, ''))) as SENDER_STATE,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"postalCode"::string, '')) as SENDER_ZIP,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[0]:"address":"countryCode"::string, ''))) as SENDER_COUNTRY,
        
        -- Clean recipient information (from packageAddress where type = 'DESTINATION')
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"addressLine1"::string, '')) as RECIPIENT_SHIP_ADDRESS1,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"addressLine2"::string, '')) as RECIPIENT_SHIP_ADDRESS2,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"city"::string, ''))) as RECIPIENT_SHIP_CITY,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"stateProvince"::string, ''))) as RECIPIENT_SHIP_STATE,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"postalCode"::string, '')) as RECIPIENT_SHIP_ZIP,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"packageAddress"[1]:"address":"countryCode"::string, ''))) as RECIPIENT_SHIP_COUNTRY_CODE,
        
        -- Clean service and package information
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"service":"description"::string, '')) as SERVICE_DESCRIPTION,
        case 
            when coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"weight":"weight"::float, 0) > 0 
            then r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"weight":"weight"::float
            else null 
        end as WEIGHT,
        trim(upper(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"weight":"unitOfMeasurement"::string, ''))) as WEIGHT_UNIT,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"deliveryInformation":"receivedBy"::string, '')) as RECEIVED_BY,
        1 as ORDER_QUANTITY,
        trim(coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"referenceNumber"[0]:"number"::string, '')) as FACILITY_ID,
        case 
            when r.DATA_JSON:"trackResponse":"shipment"[0]:"pickupDate" is not null 
            then substr(r.DATA_JSON:"trackResponse":"shipment"[0]:"pickupDate"::string, 1, 4) || '-' || 
                 substr(r.DATA_JSON:"trackResponse":"shipment"[0]:"pickupDate"::string, 5, 2) || '-' || 
                 substr(r.DATA_JSON:"trackResponse":"shipment"[0]:"pickupDate"::string, 7, 2)
            else ''
        end as SHIP_DATE_RAW,
        trim(upper(coalesce(s.value:"location":"address":"city"::string, ''))) as LOCATION,
        
        -- Metadata
        r.CREATE_DATE,
        coalesce(r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"deliveryTime":"endTime"::string, '') as DELIVERY_TIME_RAW,
        current_timestamp() as RECORD_LOAD_DATE,
        r.IS_ACTIVE
    from raw_data r,
         lateral flatten(input => r.DATA_JSON:"trackResponse":"shipment"[0]:"package"[0]:"activity") as s
    where s.value is not null
),

-- Add row number for identifying latest activity per tracking number
ranked_data as (
    select *,
           row_number() over (
               partition by CARRIER_TRACKING_NUMBER
               order by EVENT_TIME_RAW desc
           ) as RN
    from exploded_data
),

-- Mark the latest activity as active and keep all activities
deduped as (
    select *,
           case when RN = 1 then true else false end as IS_ACTIVE_FINAL
    from ranked_data
)

select
    CARRIER_TRACKING_NUMBER,
    TRANSACTION_ID,
    TRACKING_ID,
    STATUS_TYPE,
    STATUS_DESCRIPTION,
    case 
        when EVENT_TIME_RAW is not null and EVENT_TIME_RAW != '' 
        then EVENT_TIME_RAW::timestamp
        else null 
    end as EVENT_TIME,
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
    case 
        when SHIP_DATE_RAW is not null and SHIP_DATE_RAW != '' 
        then SHIP_DATE_RAW::date
        else null 
    end as SHIP_DATE,
    LOCATION,
    case 
        when CREATE_DATE is not null 
        then CREATE_DATE::timestamp
        else null 
    end as CREATE_DATE,
    case 
        when DELIVERY_TIME_RAW is not null and DELIVERY_TIME_RAW != '' 
        then DELIVERY_TIME_RAW::timestamp
        else null 
    end as DELIVERY_TIME,
    RECORD_LOAD_DATE,
    IS_ACTIVE_FINAL as IS_ACTIVE
from deduped
order by CARRIER_TRACKING_NUMBER, EVENT_TIME desc