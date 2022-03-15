insert overwrite table {1} PARTITION  (ctl_validfrom="{4}")
select
    merch_id
    ,inn
    ,gosb_id
    ,{2}    --src_ctl_loading
    ,{3}    --ctl_loading
from     	{0}
