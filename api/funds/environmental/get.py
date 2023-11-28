from main import *


def lambda_handler(event, context):
    
    fund_id = event.query('fundId')  
    client_name = event.query('clientName')
    fund = get_fund(event, fund_id, client_name)
    latest_investment_fund = get_latest_investment_fund_by_get_fund(fund)
    latest_investment_fund_assets = latest_investment_fund.get('assets', [])  
    #fund = build_get_fund_response(fund, latest_investment_fund_assets)
    
    fund_environmental = FundEnvironmentalResponse()
    list_values_latest_investment = []
    
    for asset in latest_investment_fund_assets:
        metricCO2S1 = query_metric_value(event, asset['name'], "Scope 1 GHG emissions", 2022)
        metricCO2S2 = query_metric_value(event, asset['name'], "Scope 2 GHG emissions", 2022)
        metricCO2S3 =query_metric_value(event, asset['name'], "Scope 3 GHG emissions", 2022)
        metricCO2T = query_metric_value(event, asset['name'], "Total GHG emissions", 2022)
        metricCO2S1Nm1 = query_metric_value(event, asset['name'], "Scope 1 GHG emissions", 2021)
        metricCO2S2Nm1 = query_metric_value(event, asset['name'], "Scope 2 GHG emissions", 2021)
        metricCO2S3Nm1 = query_metric_value(event, asset['name'], "Scope 3 GHG emissions", 2021)
        metricCO2TNm1 = query_metric_value(event, asset['name'], "Total GHG emissions", 2021)
        metricCO2S1Nm2 = query_metric_value(event, asset['name'], "Scope 1 GHG emissions", 2020)
        metricCO2S2Nm2 = query_metric_value(event, asset['name'], "Scope 2 GHG emissions", 2020)
        metricCO2S3Nm2 = query_metric_value(event, asset['name'], "Scope 3 GHG emissions", 2020)
        metricCO2TNm2 = query_metric_value(event, asset['name'], "Total GHG emissions", 2020)
        
        revenue = query_metric_value(event, asset['name'], "revenue", 2022)
        
        list_values_latest_investment.append({
            'id': asset.get('id'),
            'asset': asset.get('name'),
            'scope1': metricCO2S1,
            'scope2': metricCO2S2,
            'scope3': metricCO2S3,
            'total': metricCO2T,
            'scope1Nm1': metricCO2S1Nm1,
            'scope2Nm1': metricCO2S2Nm1,
            'scope3Nm1': metricCO2S3Nm1,
            'totalNm1': metricCO2TNm1,
            'scope1Nm2': metricCO2S1Nm2,
            'scope2Nm2': metricCO2S2Nm2,
            'scope3Nm2': metricCO2S3Nm2,
            'totalNm2': metricCO2TNm2,
            'industry': asset.get('financial_industry', ''),
            'asset_type': asset.get('asset_type', ''),
            'country': asset.get('country', ''),
            'amount_invested': float(asset.get('nominal_amount', '')),
            'revenue': revenue,
            'investment_fund_year': latest_investment_fund[0].get('date', '')
        })
    
    sum_of_metric = get_sum_of_metrics(list_values_latest_investment)
    
    get_values_for_scope1(fund_environmental, list_values_latest_investment, sum_of_metric)
    
    get_values_for_scope2(fund_environmental, list_values_latest_investment, sum_of_metric)
    
    get_values_for_scope3(fund_environmental, list_values_latest_investment, sum_of_metric)
    
    get_values_for_total(fund_environmental, list_values_latest_investment, sum_of_metric)
    
    return fund_environmental
    


