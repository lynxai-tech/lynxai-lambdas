from main import *


@lynx()
def lambda_handler(event, context):
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
     ORDER BY S.lastModifiedOn DESC
    """).list()

    return res
