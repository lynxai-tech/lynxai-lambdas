from main import *


@lynx()
def lambda_handler(event, context):

    fund_id_dict = {'fund_id': event.query('fundId')}
    
    event.change("""DELETE `schema`.investment_detail FROM `schema`.investment_detail
                    INNER JOIN `schema`.investment_fund ON investment_detail.fund_id = investment_fund.id
                    WHERE investment_fund.main_fund_id = (:fund_id) 
                    AND investment_detail.fund_id IN (SELECT id FROM `schema`.investment_fund WHERE main_fund_id = (:fund_id))""",
                    fund_id_dict)
    
    event.change("DELETE FROM `schema`.investment_fund WHERE main_fund_id =  (:fund_id)", fund_id_dict)
    
    event.change("DELETE FROM `schema`.main_fund WHERE id =(:fund_id)", fund_id_dict)
    
    return 'Sucess'
