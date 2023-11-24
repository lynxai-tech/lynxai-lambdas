from main import *


@lynx()
def lambda_handler(event, context):
    client_name = event.param('clientName')

    sim = Simulator(event)
    client_id = sim.get_client_id(client_name)

    res = event.select("""
    SELECT S.fund_name AS simulation_name,
           S.id AS simulation_id,
           CASE WHEN S.isDraft THEN 'draft'
           ELSE 'completed'
           END AS status,
           F.fund_name AS fund_name
      FROM `schema`.main_fund S
      LEFT JOIN `schema`.main_fund F
        ON S.simulatedMainFundId = F.id
     WHERE S.isSimulation
       AND NOT S.isDeleted
       AND S.client_id = (:clientId)
     ORDER BY S.lastModifiedOn DESC
    """, {
        'clientId': client_id
    }).list()

    return res
