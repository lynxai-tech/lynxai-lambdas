from main import *


@lynx()
def lambda_handler(event, context):
    res = event.select("""
    SELECT *
      FROM `schema`.asset
    """).list()

    return res
