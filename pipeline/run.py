from .etl import run as etl

# Sorted 

# Unsorted

def rns_cart():
    status = 'WORKED'
    tablename = 'rns_cart'
    select_data = 'cart_id, user_id, product_id, product_sku_id, store_id, cart_quantity, cart_selected, cart_note, shipping_method, shipping_fee, date_in, user_in, date_up, user_up, status_record'
    constraint_pk = 'cart_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def rns_order_product():
    status = 'WORKED'
    tablename = 'rns_order_product'
    select_data = 'order_product_id, order_id, product_id, product_sku_id, order_product_quantity, order_product_price, order_product_discount_amount, date_in, user_in, date_up, user_up, status_record'
    constraint_pk = 'order_product_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def rns_order():
    status = 'WORKED'
    tablename = 'rns_order'
    select_data = 'order_id, order_number, order_date, order_expired_date, order_status_id, store_id, payment_channel_id, user_id, date_in, user_in, date_up, user_up, status_record, order_last_update, settlement_status, settlement_date'
    constraint_pk = 'order_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def rns_order_payment():
    status = 'WORKED'
    tablename = 'rns_order_payment'
    select_data = 'order_payment_id, order_id, payment_id, invoice_number, date_in, user_in, date_up, user_up, status_record'
    constraint_pk = 'order_payment_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def rns_order_shipping():
    status = 'WORKED'
    tablename = 'rns_order_shipping'
    select_data = 'order_shipping_id, order_id, origin_name, origin_pic_name, origin_pic_phone, origin_province_id, origin_city_id, origin_district_id, origin_lower_district_id, origin_province_name, origin_city_name, origin_district_name, origin_lower_district_name, origin_postal_code, origin_latitude, origin_longitude, origin_address, origin_note, destination_name, destination_pic_name, destination_pic_phone, destination_province_id, destination_city_id, destination_district_id, destination_lower_district_id, destination_province_name, destination_city_name, destination_district_name, destination_lower_district_name, destination_postal_code, destination_latitude, destination_longitude, destination_address, destination_note, order_shipping_price, order_shipping_discount_price, order_shipping_insurance_price, order_shipping_price_insurance_price, order_shipping_receipt_number, logistics_courier_id, logistics_courier_name, logistics_courier_service_id, logistics_courier_service_name, date_in, user_in, date_up, user_up, status_record, is_seller_fleet, order_shipping_est_day_start, order_shipping_est_day_end'
    constraint_pk = 'order_shipping_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def rns_payment():
    status = 'WORKED'
    tablename = 'rns_payment'
    select_data = 'payment_id, user_id, payment_channel_id, payment_number, payment_amount, payment_created_date, payment_expired_date, payment_paid_date, payment_api_request, payment_api_response, date_in, user_in, date_up, user_up, status_record, payment_fee'
    constraint_pk = 'payment_id'
    source_pk = ''
    source_date_up = 'date_up'
    source_date_in = 'date_in'
    etl(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk)

def all():
    #Sorted


    #Unsorted
    rns_cart()
    rns_order_product()
    rns_order()
    rns_order_payment()
    rns_order_shipping()
    rns_payment()