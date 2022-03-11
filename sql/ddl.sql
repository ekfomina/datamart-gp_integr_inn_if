DROP TABLE IF EXISTS custom_salesntwrk_ul_profile.uzp_data_payroll_m;
CREATE EXTERNAL TABLE IF NOT EXISTS custom_salesntwrk_ul_profile.uzp_data_payroll_m(
    epk_id	bigint
    ,inn	bigint
    ,gosb_id int
    ,enrollment_type int
    ,amt bigint
    ,report_dt	string
    ,src_loading_id bigint
    ,ctl_loading	bigint


partitioned by (ctl_validfrom timestamp)
stored as parquet
LOCATION '/data/custom/salesntwrk/ul_profile/pa/uzp_data_payroll_m';

MSCK REPAIR TABLE custom_salesntwrk_ul_profile.stg_uzp_data_payroll_m;


DROP TABLE IF EXISTS custom_salesntwrk_ul_profile.uzp_data_merch;
CREATE EXTERNAL TABLE IF NOT EXISTS custom_salesntwrk_ul_profile.uzp_data_merch(

    merch_id	string
    ,inn	bigint
    ,gosb_id 	int
    ,src_ctl_loading	bigint
    ,ctl_loading	bigint
    ,ctl_validfrom	timestamp

)
stored as parquet
LOCATION '/data/custom/salesntwrk/ul_profile/pa/uzp_data_merch';

MSCK REPAIR TABLE custom_salesntwrk_ul_profile.uzp_data_merch
