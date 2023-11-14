from main import *


@lynx()
def lambda_handler(event, context):
    res = event.select("SELECT * from `schema`.asset").list()

    return res
