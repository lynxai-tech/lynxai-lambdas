from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    fund_id = event.param('fundId')
    simulation_id = event.param('id')
    simulation_name = event.param('name')

    sim = Simulator(event)
    # create new unnamed simulation
    new_simulation_id, snapshot_id = sim.create_unnamed_simulation(
        fund_id, prefix=f"{simulation_name} | Copy ")

    # copy all asset instances
    sim.copy_assets(fro=simulation_id, to=snapshot_id)

    return {
        'id': new_simulation_id
    }
