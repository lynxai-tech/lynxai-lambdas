from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    fund_id = event.param('id')
    name = event.param('name')

    event.change("""
    UPDATE `schema`.main_fund
       SET fund_name = (:name)
     WHERE id = (:id)
    """, {
        'id': fund_id,
        'name': name
    })

    return True
