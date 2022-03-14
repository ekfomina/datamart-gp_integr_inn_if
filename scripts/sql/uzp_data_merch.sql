insert overwrite table {1} PARTITION  ("{4}")
select
    merch_id
    ,inn
    ,gosb_id
    ,{2}    --src_ctl_loading
    ,{3}    --ctl_loading
    ,"{4}"  --ctl_validfrom
from     	{0}
