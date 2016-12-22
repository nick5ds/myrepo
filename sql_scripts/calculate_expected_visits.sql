create or replace function calulate_expected_visits(prob_list varchar(2000),transition_mult float(8),max_visits integer)
  returns float(8)
stable
as $$

    ev=0.0
    prob_list=prob_list.split(',')
    for count,prob in enumerate(prob_list):
         #calculate probability of n visits
        pn=1.0
        for i in range(count+1):
            if float(prob_list[i])*transition_mult>=1.0:
                pn=pn
            else:
                pn=pn*float(prob_list[i])*transition_mult
            end_prob=pn
        print(end_prob)
        ev=ev+pn
      #using the calculation for the nth visis, calculates probability of future visits
    if max_visits>len(prob_list):
       for i in range(len(prob_list),max_visits):
           ev=ev+end_prob
    return ev
$$ language plpythonu;


select aw,possible_visits,grouping,total_users,total_visits
,calulate_expected_visits(trans,1.0,0::int) as expected_visits
,calulate_expected_visits(trans,0.9,0::int) as "expected_visitsx0.9"
,calulate_expected_visits(trans,0.95,0::int) as "expected_visitsx0.95"
,calulate_expected_visits(trans,1.01,0::int) as "expected_visitsx1.01"
,calulate_expected_visits(trans,1.05,0::int) as "expected_visitsx1.05"
,calulate_expected_visits(trans,1.1,0::int) as "expected_visitsx1.1"
,calulate_expected_visits(trans,1.2,0::int) as "expected_visitsx1.2"
--,trans
 from
(select  aw
,grouping
,possible_visits
,visits_in_a_year
,max(users) total_users
,sum(users) as total_visits
,listagg(round(users*1.0/prev,4),',') within group( order by visit_number) trans
from

(select *,coalesce(lag(users,1) over (partition by aw,grouping order by visit_number),users) as prev from
(select aw, possible_visits,365 visits_in_a_year,visit_number,grouping,count(distinct user_account) users

from
(select a.user_account,visit_number,round(aw,0) as aw,grouping
from customer_identity.t_user_activity_stats a  

join
(select distinct user_account
,lifetime_avg_visit_interval aw
from customer_identity.t_user_activity_stats 
where activity_date=activation_date_3_rd_visit
 and activation_date_3_rd_visit>'2015-10-01' 
and activation_date_3_rd_visit<='2015-12-18' 
and is_test_user=false and is_employee_user=false
and first_store not like '%Topanga%' and activity_type!='inactive' and datediff('day',activation_date_3_rd_visit,activity_date) between 0 and 365) avg_window
on (a.user_account=avg_window.user_account)
join (
select  user_account, 'all' as grouping--,case when random()<=.8 then 'train' else 'test' end as grouping 
from 
(select distinct user_account from
customer_identity.t_user_accounts)) c
on (a.user_account=c.user_account)
where  aw>0 
and activation_date_3_rd_visit>'2015-10-01' 
and activation_date_3_rd_visit<='2015-12-18'and is_test_user=false and is_employee_user=false
and datediff('day',activation_date_3_rd_visit,activity_date) between 0 and 365
and first_store not like '%Topanga%' and activity_type!='inactive') 
group by 1,2,3,4,5)
)
group by 1,2,3,4
)



