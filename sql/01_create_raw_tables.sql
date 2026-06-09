\connect manufacturing_dw

create table if not exists raw_sensor_data (
    event_time timestamp not null,
    factory_id varchar(20),
    line_id varchar(20),
    machine_id varchar(20) not null,
    product_id varchar(20),
    mode varchar(20),
    temperature numeric(8, 2) not null,
    pressure numeric(8, 2) not null,
    vibration numeric(8, 3) not null,
    motor_current numeric(8, 2),
    rpm integer,
    anomaly_type varchar(50),
    sequence integer
);

create table if not exists raw_production_data (
    production_date date not null,
    lot_id varchar(40) primary key,
    machine_id varchar(20) not null,
    product_id varchar(20) not null,
    shift varchar(10),
    planned_quantity integer,
    quantity integer not null,
    cycle_time_sec numeric(8, 2),
    operator_name varchar(100)
);

create table if not exists raw_quality_data (
    inspection_time timestamp not null,
    lot_id varchar(40) not null,
    result varchar(10) not null,
    defect_type varchar(50),
    sample_size integer,
    defect_count integer
);
