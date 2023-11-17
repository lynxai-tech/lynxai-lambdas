from datetime import datetime


class Simulator:
    def __init__(self, event):
        self.event = event

    def create_unnamed_simulation(self, fund_id, prefix="Unnamed simulation "):
        res = self.event.select(f"""
        SELECT fund_name
        FROM `schema`.main_fund
        WHERE isSimulation
        AND fund_name LIKE '{prefix}%'
        """).list()

        max_unnamed = max([int(x['fund_name'].replace(prefix, '').strip())
                           for x in res]) if res else 0

        res = self.event.change("""
        INSERT INTO `schema`.main_fund (fund_name, lastModifiedOn, isSimulation, simulatedMainFundId, isDraft)
        VALUES (:fund_name, :lastModifiedOn, :isSimulation, :simulatedMainFundId, :isDraft)
        """, {
            'fund_name': f'{prefix}{max_unnamed+1}',
            'lastModifiedOn': datetime.utcnow(),
            'isSimulation': True,
            'simulatedMainFundId': fund_id,
            'isDraft': True
        })

        simulation_id = res['generatedFields'][0]['longValue']

        res1 = self.event.change("""
        INSERT INTO `schema`.investment_fund (main_fund_id, fund_name)
        VALUES (:mainFundId, :fundName)
        """, {
            'mainFundId': simulation_id,
            'fundName': f'Unnamed simulation {max_unnamed+1}'
        })

        snapshot_id = res1['generatedFields'][0]['longValue']

        return simulation_id, snapshot_id

    def copy_assets(self, fro, to):
        snapshot_id = self.event.select("""
        SELECT MAX(IVF.id)
        FROM `schema`.main_fund F
        LEFT JOIN `schema`.investment_fund IVF
            ON IVF.main_fund_id = F.id
        WHERE F.id = (:mainFundId)
        """, {
            'mainFundId': fro
        }).val()

        assets = self.event.select("""
        SELECT ID.*
        FROM `schema`.investment_fund IVF
        LEFT JOIN `schema`.investment_detail ID
            ON ID.fund_id = IVF.id
        WHERE IVF.id = (:investmentFundId)""", {
            'investmentFundId': snapshot_id
        }).list()

        def transform_asset(asset):
            return asset | {'fund_id': to}

        self.event.change("""
        INSERT INTO `schema`.investment_detail (fund_id, asset_id, investment_type, nominal_amount, portfolio_asset_weight_non_financial, date, currency, share_amount)
        VALUES (:fund_id, :asset_id, :investment_type, :nominal_amount, :portfolio_asset_weight_non_financial, :date, :currency, :share_amount)
        """, [transform_asset(x) for x in assets if x['id'] is not None])
