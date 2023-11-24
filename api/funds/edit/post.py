from main import *


@lynx()
def lambda_handler(event, context):

    event_body = event.body()
    
    remove_deleted_fund_versions(event)
    
    edit_fund(event)

    event.change("""UPDATE `schema`.main_fund SET fund_name = (:fund_name), fund_type = (:), country = (:country), status = (:status), amount = (:amount), portfolio_weight_percentage = (:portfolio_weight_percentage) WHERE id = (:fund_id)""",
                 {
                     'fund_name':  event_body['fundName'],
                     'fund_type':  event_body['fundType'],
                     'country':  event_body['country'],
                     'status':  event_body['status'],
                     'amount':  event_body['amount'],
                     'portfolio_weight_percentage':  event_body['portfolioWeightPercentage'],
                     'fund_id': event_body['fundId']
                 })
    
    
    
    
