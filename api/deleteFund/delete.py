from main import *



@lynx()
def lambda_handler(event, context):
    res = event.call("""CALL delete_funds(:fundName)""", ).list()

    return res