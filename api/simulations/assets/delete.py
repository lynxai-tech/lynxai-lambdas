from main import *


@lynx()
def lambda_handler(event, context):
    event.change("""
    DELETE FROM `schema`.investment_detail
     WHERE id = (:id)
    """, {
        'id': event.param('id')
    })

    return True
