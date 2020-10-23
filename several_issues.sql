drop table if exists dm_si.fd_tmp;
create table dm_si.fd_tmp
as
select
    explode(
        map(
            '2020-01-01', 90
            ,'2020-01-02', 101
            ,'2020-01-03', 106
            ,'2020-01-04', 109
            ,'2020-01-05', 89
            ,'2020-01-06', 106
            ,'2020-01-07', 109
            ,'2020-01-08', 89
            )
        );
;
set hive.support.quoted.identifiers=None;
select
    tmp.`^((?!flag).)*$`
from(
    select
        *
        ,if(flag2 is not null,count(1) over (partition by flag1-flag2),null) flag3
    from(
        select
            *
            ,row_number() over (order by key asc) flag1
            ,if(value>100,row_number() over (partition by if(value>100,1,null) order by key asc),null) flag2
        from
            dm_si.fd_tmp
        ) tmp
    ) tmp
where
    flag3>=3
order by
    key asc
;
