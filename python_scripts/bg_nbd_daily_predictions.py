from bg_nbd_model_with_projections_and_elasticity import *
from datetime import datetime, timedelta

def generate_modifier_data(end_date,**kwargs):

#generates the base data needed for bg_nbd. Note that the interval sets the period length of the model



    conn = psycopg2.connect(
        dbname=kwargs['FIVETRAN_DATABASE'],
        host=kwargs['FIVETRAN_HOST'],
        port=5439,
        user=kwargs['FIVETRAN_USER'],
        password=kwargs['FIVETRAN_PASSWORD']
    )
    conn.autocommit = True
    query="""



select a.user_account as id,modifier_name,modifier_id,num_mods*1.0/total_orders as mod_purchase_rate from
(
select ua.user_account 
,modifier_id
,m.name as modifier_name
,count(distinct mo.id) as num_mods
from kwwhub_public.modifications mo
join kwwhub_public.modifiers m on
(mo.modifier_id=m.id and mo.created_at between m.effective_date and m.expiration_date)
join kwwhub_public.line_items li on ( mo.line_item_id=li.id)
join kwwhub_public.items i on (li.item_id=i.id
and li.created_at between i.effective_date and i.expiration_date)
join kwwhub_public.orders o
on (li.order_id=o.id)
join customer_identity.t_user_accounts ua
on (ua.user_id=o.user_id)

where o.status=500
and o.delivered_at<='2016-10-01'
group by 1,2,3
) a
join
(select user_account,count(distinct created_at::date) as total_orders from 
kwwhub_public.orders o
join customer_identity.t_user_accounts ua
on (ua.user_id=o.user_id)
where o.status=500
and o.delivered_at<='{0}' and is_test_user=false and is_employee_user=false 
group by 1) b
on (a.user_account=b.user_account )

    """.format(end_date)
    mod_data = pd.read_sql(query, conn)
    return mod_data

def mod_sales_predictions(start_date,end_date):
    final=DataFrame()
    while start_date<end_date:
        date_str=datetime.strftime(start_date,'%Y-%m-%d')
        print date_str
        data=generate_ndb_data(start_date,'day',FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
        mods=generate_modifier_data(start_date,FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
        model=fit_nbd_model(data)
        preds=make_preds(model, data, 1)
        preds_data=preds['data'].set_index('id',drop=True,verify_integrity=False)
        mods_data=mods.set_index('id',drop=True,verify_integrity=False)
        joined_data=preds_data.join(mods_data)
        joined_data['projected_element_sales']=joined_data.projected_transactions_next_1_periods*joined_data.mod_purchase_rate
      #  mod_sales=joined_data.groupby(['modifier_id','modifier_name'])['projected_element_sales'].sum()
        mod_sales=DataFrame({'count' : joined_data.groupby( ['modifier_id','modifier_name' ] )['projected_element_sales'].sum()}).reset_index()
        mod_sales['date']=date_str
        final=final.append(mod_sales)
        print final.head()
        start_date=start_date+timedelta(days=1)
#        date_str=datetime.strftime(start_date,'%Y-%m-%d')
#        print date_str
#        data=generate_ndb_data(start_date,'day',FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
#        mods=generate_modifier_data(start_date,FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
#        model=fit_nbd_model(data)
#        pred_data=data
#        preds=make_preds(model,pred_data, 1)
#        preds_data=preds['data'].set_index('id',drop=True,verify_integrity=False)
#        mods_data=mods.set_index('id',drop=True,verify_integrity=False)
#        joined_data=preds_data.join(mods_data)
#        joined_data['projected_element_sales']=joined_data.projected_transactions_next_1_periods*joined_data.mod_purchase_rate
#        mod_sales=joined_data.groupby(['modifier_id','modifier_name'])['projected_element_sales'].sum()
#        mod_sales['date']=date_str
#        final.append(mod_sales)
#        print final.head()
#        start_date=start_date+timedelta(days=1)
    return final

if __name__ == "__main__":
    with open('../config.json') as config:
        conf=json.load(config)
    redshift=conf['redshift']
    FIVETRAN_HOST=redshift['FIVETRAN_HOST']
    FIVETRAN_USER = redshift['FIVETRAN_USER']
    FIVETRAN_PASSWORD = redshift['FIVETRAN_PASSWORD']
    FIVETRAN_DATABASE = redshift['FIVETRAN_DATABASE']
    discount_range=[0,3,0.25]
    bowl_distribution=[0.139 , 0.188, 0.142, 0.103, 0.427]
    avg_price=6.7
    old_price=7.85
    elastic_range=[-4, 0, 0.1]
    start_date=datetime(2016,10,01)
    end_date=datetime(2016,11,01)
    preds=mod_sales_predictions(start_date,end_date)
    preds.to_csv('../mod_preds.csv')
#    preds=pd.read_csv('../mod_sales.csv')
#    preds['date']='2016-10-01'
#    print preds.head()
#    final=DataFrame()
#    final=final.append(preds)
#    print final.head()

#   data=generate_ndb_data('2016-10-01','day',FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
#   mods=generate_modifier_data('2016-10-01',FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
#   model=fit_nbd_model(data)
#   pred_data=data
#   preds=make_preds(model,pred_data, 1)
#   preds_data=preds['data'].set_index('id',drop=True,verify_integrity=False)
#   mods_data=mods.set_index('id',drop=True,verify_integrity=False)
#   #preds_data.to_csv('../base_projection.csv',index=True)
#   #mods_data.to_csv('../mods.csv',index=True)
#   joined_data=preds_data.join(mods_data)
#   joined_data['projected_element_sales']=joined_data.projected_transactions_next_1_periods*joined_data.mod_purchase_rate
#   #joined_data.to_csv('../joined_data.csv',index=True)
#   mod_sales=joined_data.groupby(['modifier_id','modifier_name'])['projected_element_sales'].sum()
#
#   mod_sales.to_csv('../mod_sales.csv')
