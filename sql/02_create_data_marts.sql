\connect manufacturing_dw

drop table if exists dm_daily_production;
create table dm_daily_production as
select
    production_date,
    machine_id,
    product_id,
    sum(quantity) as total_quantity
from raw_production_data
group by production_date, machine_id, product_id;

drop table if exists dm_daily_quality;
create table dm_daily_quality as
select
    p.production_date,
    p.machine_id,
    count(q.lot_id) as total_lot_count,
    count(*) filter (where q.result = 'FAIL') as defect_lot_count,
    round(count(*) filter (where q.result = 'FAIL')::numeric / nullif(count(q.lot_id), 0), 4) as defect_rate
from raw_quality_data q
join raw_production_data p on q.lot_id = p.lot_id
group by p.production_date, p.machine_id;

drop table if exists dm_machine_health;
create table dm_machine_health as
select
    event_time::date as event_date,
    machine_id,
    round(avg(temperature), 2) as avg_temperature,
    round(avg(pressure), 2) as avg_pressure,
    round(avg(vibration), 3) as avg_vibration,
    case
        when avg(temperature) >= 82 or avg(pressure) >= 5.0 or avg(vibration) >= 0.25 then 'DANGER'
        when avg(temperature) >= 78 or avg(pressure) >= 4.7 or avg(vibration) >= 0.21 then 'WARNING'
        else 'NORMAL'
    end as health_status
from raw_sensor_data
group by event_time::date, machine_id;
