from main import *


@lynx()
def lambda_handler(event, context):

    event_body = event.body()
    
    print(1)
    
    remove_deleted_fund_versions(event)
    print(1)
    
    edit_fund(event)
    print(1)

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
    
    
    
    
