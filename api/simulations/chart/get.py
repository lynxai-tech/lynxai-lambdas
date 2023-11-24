from main import *
2

@lynx()
def lambda_handler(event, context):
    client_name = event.param('clientName')

    sim = Simulator(event)
    client_id = sim.get_client_id(client_name)

    # find all simulation assets, get id, start and end date

    # get the metric tCO2/y for all assets from simulation, for each year

    # sum all for each year

    # assume 2022 is point zero

    # find the envvar REDUCTION_LIMIT_PERC_2030


    return res
