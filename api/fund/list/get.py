from main import *


@lynx()
def lambda_handler(event, context):
    res = event.select("""
    SELECT i.id,
           i.date,
           if2.id if_id,
           i.fund_name,
           i.fund_type,
           i.eu_sfdr_article,
           i.country,
           i.status,
           i.amount,
           i.portfolio_weight_percentage,
           i.client_id,
           COUNT(id.asset_id) nr_assets
      FROM `schema`.main_fund i
      JOIN `schema`.client u
        ON u.id = i.client_id
      JOIN `schema`.investment_fund if2
        ON if2.main_fund_id = i.id
      LEFT JOIN `schema`.investment_detail id
        ON id.fund_id = if2.id
     WHERE u.name = (:clientName)
       AND i.date = if2.date
     GROUP BY if2.id;
    """, {'clientName': event.query('clientName')}).list()
    
    print(res)

    return res
