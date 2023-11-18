from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    fund_id = event.param('fundId')
    client_name = event.param('clientName')

    sim = Simulator(event)
    client_id = sim.get_client_id(client_name)
    # create new unnamed simulation
    simulation_id, snapshot_id = sim.create_unnamed_simulation(fund_id, client_id=client_id)

    # copy all asset instances
    sim.copy_assets(fro=fund_id, to=snapshot_id)


    return {
        'simulationId': simulation_id
    }
