{{ config(
    materialized='incremental',
    unique_key='CARRIER_TRACKING_NUMBER',
    on_schema_change='fail',
    schema='dwh_layer'
) }}

with base as (
    select *
    from {{ ref('ups_base') }}
    where IS_ACTIVE = true
    {% if is_incremental() %}
        and EVENT_TIME > (select max(EVENT_TIME) from {{ this }})
    {% endif %}
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
    CREATE_DATE,
    DELIVERY_TIME,
    current_timestamp() as RECORD_LOAD_DATE,
    IS_ACTIVE
from base
order by CARRIER_TRACKING_NUMBER, EVENT_TIME desc