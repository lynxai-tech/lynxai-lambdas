from main import *



@lynx()
def lambda_handler(event, context):
  
    
    res = event.call('`schema`.delete_funds',{'fundName': event.query('fundName')})
    # mainFundId = event.select("""SELECT id FROM `schema`.main_fund WHERE fund_name = (:fundName) LIMIT 1;""",{'fundName': event.query('fundName')}).list()
  
    
    # print(res)
    
    return res
  
  
  
  