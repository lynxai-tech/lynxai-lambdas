from main import *


@lynx()
def lambda_handler(event, context):
  
  client_name = event.query('clientName')
  
  res = get_fund_list(event, client_name)
    
  print(res)

  return res
