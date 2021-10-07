select *, "${schema_bicom}" as schema_bicom, "${bucket_data_lake}" as bucket_data_lake
from bicomdb.oag_type_3_flight_leg 
limit 10;

CREATE EXTERNAL TABLE IF NOT EXISTS ${schema_bicom}.oag_type_3_flight_leg_processed(
`record_type` int, 
`operational_suffix` string, 
`airline_designator` string, 
`flight_number` string, 
`itinerary_variation_id` int, 
`num_sequence_leg` int, 
`service_type` string, 
`operational_period_from` date, 
`operational_period_since` date, 
`days_operation` string, 
`frequency_rate` string, 
`departure_station` string, 
`sched_time_dep_pas` string, 
`sched_time_dep_aircraft` string, 
`utc_local_time_variation_dep_stn` string, 
`terminal_pas_dep_stn` string, 
`arrival_station` string, 
`sched_time_arr_aircraft` string, 
`sched_time_arr_pas` string, 
`utc_local_time_variation_arr_stn` string, 
`terminal_pas_arr_stn` string, 
`aircraft_type` string, 
`pas_reservation_bkg_designator` string, 
`pas_reservation_bkg_modifier` string, 
`meal_service_note` string, 
`joint_operation_airline` string, 
`min_connecting_time` string, 
`itinerary_variation_id_overflow` string, 
`aircraft_owner` string, 
`airline_op` string, 
`cockpit_crew_employer` string, 
`cabin_crew_employer` string, 
`airline_designator2` string, 
`flight_number2` string, 
`airline_rotation_layover` string, 
`operational_suffix2` string, 
`flight_transit_layover` string, 
`operating_airline_disclosure` string, 
`traffic_restriction_code` string, 
`traffic_restriction_code_leg_overflow_ind` string, 
`aircraft_config` string, 
`date_variation` string, 
`num_record_serial` string, 
`snapshot_date` string, 
`file_date` string, 
`seats_f` string, 
`seats_c` string, 
`seats_w` string, 
`seats_y` string, 
`change_utc_dep` int, 
`change_utc_arr` int, 
`covid` string, 
`operational_date` date, 
`weekday` string, 
`weekday_name` string, 
`city_name_o` string, 
`ctry_code_o` string, 
`continent_name_o` string, 
`city_name_d` string, 
`ctry_code_d` string, 
`continent_name_d` string, 
`market_segment_code` string, 
`departure_region` string, 
`arrival_region` string)
PARTITIONED BY ( 
`insert_date_ci` string)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
's3a://${bucket_data_lake}/hive/data/bicom/oag/oag_type3/flight_leg_processed'
TBLPROPERTIES (
'bucketing_version'='2', 
'discover.partitions'='true', 
'transient_lastDdlTime'='1615201860');

with filiales as (
    select 
        *,
        cast(COALESCE(regexp_replace(regexp_replace(regexp_replace(COALESCE(split(aircraft_config, 'F')[1], '0'), 'C\\d*', ''), 'W\\d*', ''), 'Y\\d*', ''), '0') as int) as seats_f,
        cast(COALESCE(regexp_replace(regexp_replace(COALESCE(split(aircraft_config, 'C')[1], '0'), 'W\\d*', ''), 'Y\\d*', ''), '0') as int) as seats_c,
        cast(COALESCE(split(aircraft_config, 'Y')[1], '0') as int) as seats_y,
        cast(COALESCE(regexp_replace(COALESCE(split(aircraft_config, 'W')[1], '0'), 'Y\\d*', ''), '0') as int) as seats_w,
        1 - case when SUBSTRING(utc_local_time_variation_dep_stn, 0, 1) = '+' then
            cast(SUBSTRING(utc_local_time_variation_dep_stn, 2, 2) as int)
        else
            cast(SUBSTRING(utc_local_time_variation_dep_stn, 2, 2) as int) * (-1)
        end as change_utc_dep, 
        1 - case when SUBSTRING(utc_local_time_variation_arr_stn, 0, 1) = '+' then
            cast(SUBSTRING(utc_local_time_variation_arr_stn, 2, 2) as int)
        else
            cast(SUBSTRING(utc_local_time_variation_arr_stn, 2, 2) as int) * (-1)
        end as change_utc_arr,
        case when file_date = '2020-02' then 
                    'pre-covid'
        else
                    'post-covid'
        end as covid
    from 
        ${schema_bicom}.oag_type_3_flight_leg 
    where
        (file_date = "${current_insert_date_ci}")
        and coalesce(operating_airline_disclosure, 'a') != 'Z'
        and coalesce(operating_airline_disclosure, 'a') != 'L'
), p as (
    select 
        *,
        cast(COALESCE(regexp_replace(regexp_replace(regexp_replace(COALESCE(split(aircraft_config, 'F')[1], '0'), 'C\\d*', ''), 'W\\d*', ''), 'Y\\d*', ''), '0') as int) as seats_f,
        cast(COALESCE(regexp_replace(regexp_replace(COALESCE(split(aircraft_config, 'C')[1], '0'), 'W\\d*', ''), 'Y\\d*', ''), '0') as int) as seats_c,
        cast(COALESCE(split(aircraft_config, 'Y')[1], '0') as int) as seats_y,
        cast(COALESCE(regexp_replace(COALESCE(split(aircraft_config, 'W')[1], '0'), 'Y\\d*', ''), '0') as int) as seats_w,
        1 - case when SUBSTRING(utc_local_time_variation_dep_stn, 0, 1) = '+' then
            cast(SUBSTRING(utc_local_time_variation_dep_stn, 2, 2) as int)
        else
            cast(SUBSTRING(utc_local_time_variation_dep_stn, 2, 2) as int) * (-1)
        end as change_utc_dep, 
        1 - case when SUBSTRING(utc_local_time_variation_arr_stn, 0, 1) = '+' then
            cast(SUBSTRING(utc_local_time_variation_arr_stn, 2, 2) as int)
        else
            cast(SUBSTRING(utc_local_time_variation_arr_stn, 2, 2) as int) * (-1)
        end as change_utc_arr,
        case when file_date = '2020-02' then 
            'pre-covid'
        else
            'post-covid'
        end as covid
    from 
        ${schema_bicom}.oag_type_3_flight_leg 
    where
        ( file_date = "${current_insert_date_ci}")
        and ((aircraft_owner != '' and airline_designator = aircraft_owner)
                        or aircraft_owner = '')
        and coalesce(operating_airline_disclosure, 'a') != 'Z'
        and coalesce(operating_airline_disclosure, 'a') != 'L'
    UNION
        select filiales.*
        from filiales 
        inner join
        ${schema_bicom}.dict_filiales_capacidad as filiales_dict
        on filiales.airline_designator = filiales_dict.airline_designator
        and filiales.aircraft_owner = filiales_dict.aircraft_owner
), oag as (
    select 
        p.*,
        case when 
            aircraft_owner = ''
        then
            airline_designator
        else
            aircraft_owner
        end as airline_op,
        date_add (operational_period_from,i)   as operational_date,
        from_unixtime(unix_timestamp(date_add (operational_period_from,i)), 'u') as weekday,
        from_unixtime(unix_timestamp(date_add (operational_period_from,i)), 'EEEE') as weekday_name
    from p
    lateral view
    posexplode(split(space(datediff(coalesce(
                                            p.operational_period_since,
                                            date(cast(cast(SUBSTRING(p.file_date,0,4)as int) + 1 as string) || substring(p.file_date, 5, 3) || '-01')
                                            ),
                                    p.operational_period_from)
                            ),
                    ' ')
                ) pe as i,x
    where 
        (days_operation like '%' || from_unixtime(unix_timestamp(date_add (operational_period_from,i)), 'u') || '%')
    --and ((date_add(operational_period_from,i) >= add_months(date(snapshot_date), 1)
    --        and date_add(operational_period_from,i) < add_months(date(snapshot_date), 2)
    --        and file_date != '2020-05')
    --  or (file_date = '2020-05' 
    --      and date_add(operational_period_from,i) >= add_months(date(snapshot_date), 1))
), seats as (
    select 
        aircraft_owner,
        subfleet,
        first_seats,
        business_seats,
        prem_econ_seats,
        disc_econ_seats
    from ${schema_bicom}.aircraft_configuration_competitors
), countries as (
    select
        stn_code_o, 
        city_name_o,
        ctry_code_o,
        continent_name_o,
        stn_code_d, 
        city_name_d,
        ctry_code_d, 
        continent_name_d,
        market_segment_code
    from ${schema_bicom}.dmc_lk_market_route
)
INSERT overwrite TABLE ${schema_bicom}.oag_type_3_flight_leg_processed PARTITION (insert_date_ci="${current_insert_date_ci}")
select distinct
oag.record_type,
oag.operational_suffix,
oag.airline_designator,
case when length(oag.flight_number) = 2 
then '00' || oag.flight_number 
when length(oag.flight_number) = 3
then '0' || oag.flight_number
else oag.flight_number end as flight_number,
oag.itinerary_variation_id,
oag.num_sequence_leg,
oag.service_type,
oag.operational_period_from,
oag.operational_period_since,
oag.days_operation,
oag.frequency_rate,
oag.departure_station,
oag.sched_time_dep_pas,
oag.sched_time_dep_aircraft,
oag.utc_local_time_variation_dep_stn,
oag.terminal_pas_dep_stn,
oag.arrival_station,
oag.sched_time_arr_aircraft,
oag.sched_time_arr_pas,
oag.utc_local_time_variation_arr_stn,
oag.terminal_pas_arr_stn,
oag.aircraft_type,
oag.pas_reservation_bkg_designator,
oag.pas_reservation_bkg_modifier,
oag.meal_service_note,
oag.joint_operation_airline,
oag.min_connecting_time,
oag.itinerary_variation_id_overflow,
oag.aircraft_owner,
oag.airline_op,
oag.cockpit_crew_employer,
oag.cabin_crew_employer,
oag.airline_designator2,
substring(oag.flight_number, 2) as flight_number2,
oag.airline_rotation_layover,
oag.operational_suffix2,
oag.flight_transit_layover,
oag.operating_airline_disclosure,
oag.traffic_restriction_code,
oag.traffic_restriction_code_leg_overflow_ind,
oag.aircraft_config,
oag.date_variation,
oag.num_record_serial,
oag.snapshot_date,
case when from_unixtime(unix_timestamp(to_date(oag.file_date)), 'u') = 6
then
    cast(date_sub(date("${current_insert_date_ci}"),pmod(datediff(date("${current_insert_date_ci}"),'1900-01-07'),7) + 7) as string)
else 
    cast(date_sub(date("${current_insert_date_ci}"),pmod(datediff(date("${current_insert_date_ci}"),'1900-01-07'),7)) as string)
end as file_date,
case when (oag.seats_f = 0 
            and oag.seats_w = 0 
            and oag.seats_y = 0
            and oag.seats_c = 0) then
    seats.first_seats
else
    oag.seats_f
end as seats_f,
case when (oag.seats_f = 0 
            and oag.seats_w = 0 
            and oag.seats_y = 0
            and oag.seats_c = 0) then
    seats.business_seats
else
    oag.seats_c
end as seats_c,
case when (oag.seats_f = 0 
            and oag.seats_w = 0 
            and oag.seats_y = 0
            and oag.seats_c = 0) then
    seats.prem_econ_seats
else
    oag.seats_w
end as seats_w,
case when (oag.seats_f = 0 
            and oag.seats_w = 0 
            and oag.seats_y = 0
            and oag.seats_c = 0) then
    seats.disc_econ_seats
else
    oag.seats_y
end as seats_y,
oag.change_utc_dep,
oag.change_utc_arr,
oag.covid,
oag.operational_date,
oag.weekday,
oag.weekday_name,
air1.city_code as city_name_o,
air1.country_cd as ctry_code_o,
air1.oag_reg_name as continent_name_o,
air2.city_code as city_name_d,
air2.country_cd as ctry_code_d, 
air2.oag_reg_name as continent_name_d,
case when 
    air1.country_cd == air2.country_cd
THEN
    'SH'
when 
    (air1.subregion in ("Africa : North Africa", "Middle East") and air2.iag_region == "Europe")
    or 
    (air2.subregion in ("Africa : North Africa", "Middle East") and air1.iag_region == "Europe")
    or
    air1.iag_region == air2.iag_region
then 
    'MH'
ELSE
    'LH'
end as market_segment_code,
air1.iag_region as departure_region, 
air2.iag_region as arrival_region
from oag
left join seats
    on oag.airline_op = seats.aircraft_owner
    and oag.aircraft_type = seats.subfleet
LEFT JOIN ${schema_bicom}.iag_airport_ref as air1
    on air1.stn_cd = oag.departure_station
LEFT JOIN ${schema_bicom}.iag_airport_ref as air2
    on air2.stn_cd = oag.arrival_station
left join countries
    on oag.departure_station = countries.stn_code_o
    and oag.arrival_station = countries.stn_code_d;
    
