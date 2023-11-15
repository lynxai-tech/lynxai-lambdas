from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    fund_id = event.param('fundId')

    res = event.select("""
    SELECT fund_name
      FROM `schema`.main_fund
     WHERE isSimulation
       AND fund_name LIKE 'Unnamed simulation%'
    """).list()

    max_unnamed = max([int(x['fund_name'].replace('Unnamed simulation', '').strip())
                       for x in res]) if res else 0

    res = event.change("""
    INSERT INTO `schema`.main_fund (fund_name, date, isSimulation, simulatedMainFundId, isDraft)
    VALUES (:fund_name, :date, :isSimulation, :simulatedMainFundId, :isDraft)
    """, {
        'fund_name': f'Unnamed simulation {max_unnamed+1}',
        'date': datetime.utcnow(),
        'isSimulation': True,
        'simulatedMainFundId': fund_id,
        'isDraft': True
    })

    return {
        'simulationId': res['generatedFields'][0]['longValue']
    }
