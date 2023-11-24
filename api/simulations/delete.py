from main import *


@lynx()
def lambda_handler(event, context):
    event.change("""
    UPDATE `schema`.main_fund
       SET isDeleted = TRUE
     WHERE id = (:id)
       AND isSimulation
    """, {
        'id': event.param('id')
    })

    return True
