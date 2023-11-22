from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    event.change("""
    UPDATE `schema`.main_fund
       SET isDraft = 0
     WHERE id = (:id)
    """, {
        'id': event.param('id')
    })

    return True