from main import *


@lynx()
def lambda_handler(event, context):
    res = event.select("""
    SELECT *
      FROM `schema`.asset
     WHERE id = :assetId
    """, {
        'assetId': event.param('id')
    }).dict()

    return res
