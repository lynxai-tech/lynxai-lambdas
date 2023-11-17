from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    fund_id = event.param('fundId')

    sim = Simulator(event)
    # create new unnamed simulation
    simulation_id, snapshot_id = sim.create_unnamed_simulation(fund_id)

    # copy all asset instances
    sim.copy_assets(fro=fund_id, to=snapshot_id)


    return {
        'simulationId': simulation_id
    }
