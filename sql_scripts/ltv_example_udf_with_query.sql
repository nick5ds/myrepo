create or replace function create_ltv (user_activity varchar(2000), days_old integer)
  returns integer
stable
as $$
    from datetime import datetime
    ltv=0
    activity=dict(item.split(":") for item in user_activity.split(","))
    activity_dates=list(activity.keys())
    activity_dates.sort()
    install_date=datetime.strptime(min(activity_dates),"%Y-%m-%d")
    for day in activity_dates:
        if (datetime.strptime(day,"%Y-%m-%d")-install_date).days>=days_old:
            break
        if (datetime.strptime(day,"%Y-%m-%d")-install_date).days<=days_old:
            ltv+=int(activity[day])
    return ltv
$$ language plpythonu;


select user_id,create_ltv(activity,30) from (
select user_id,listagg(created_at::varchar(10)||':'||total::varchar(6),',') as activity from(
select user_id,created_at::date,sum(total) total from kwwhub_public.orders group by 1,2 order by 1,2 limit 1000)
group by 1)
