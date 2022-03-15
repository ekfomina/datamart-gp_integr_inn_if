insert overwrite table {1} PARTITION  (ctl_validfrom="{4}")
select
    epk_id
    ,inn
    ,gosb_id
    ,enrollment_type
    ,amt
    ,report_dt
    ,{2}    --src_ctl_loading
    ,{3}    --ctl_loading
from     	{0}
