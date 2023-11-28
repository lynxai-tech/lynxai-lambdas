from typing import List, Dict, Optional
from dataclasses import dataclass
from fund_dictionaries import dictionary_countries, dictionary_asset_types, dictionary_investment_types


@dataclass
class FundEnvironmentalMainResponseObject:
    bestAssetsPerEurInvested: List[dict] = []
    bestAssetsPerRevenue: List[dict] = []
    bestIndustriesPerEurInvested: List[dict] = []
    bestIndustriesPerRevenue: List[dict] = []
    worstAssetsPerEurInvested: List[dict] = []
    worstAssetsPerRevenue: List[dict] = []
    worstIndustriesPerEurInvested: List[dict] = []
    worstIndustriesPerRevenue: List[dict] = []
    totalEmissionData: List[dict] = []
    emissionsIntensityPerRevenue: List[dict] = []
    emissionsIntensityPerEurInvested: List[dict] = []
    co2PerAssetType: List[dict] = []
    co2PerCountry: List[dict] = []
    co2PerIndustry: List[dict] = []


@dataclass
class FundEnvironmentalSpecificResponseObject:
    bestAssetsPerEurInvested: List[dict] = []
    bestAssetsPerRevenue: List[dict] = []
    bestIndustriesPerEurInvested: List[dict] = []
    bestIndustriesPerRevenue: List[dict] = []
    worstAssetsPerEurInvested: List[dict] = []
    worstAssetsPerRevenue: List[dict] = []
    worstIndustriesPerEurInvested: List[dict] = []
    worstIndustriesPerRevenue: List[dict] = []
    emissionData: List[dict] = []
    thresholdLastYear: List[dict] = []
    highestEmissionValue: Optional[float] = None
    lowestEmissionValue: Optional[float] = None


@dataclass
class FundEnvironmentalResponse:
    CO2T: FundEnvironmentalMainResponseObject = FundEnvironmentalMainResponseObject()
    CO2S1: FundEnvironmentalSpecificResponseObject = FundEnvironmentalSpecificResponseObject()
    CO2S2: FundEnvironmentalSpecificResponseObject = FundEnvironmentalSpecificResponseObject()
    CO2S3: FundEnvironmentalSpecificResponseObject = FundEnvironmentalSpecificResponseObject()


def get_lowest_assets_for_size_per_property(list_of_values_of_latest_investment: List[Dict], size: str, property: str) -> List[Dict]:
    filtered_list = filter(lambda item: item[size] is not None and item[size]
                           != 0 and item[property] is not None, list_of_values_of_latest_investment)
    mapped_list = map(lambda item: {
                      "label": item["asset"], "value": item[size] / item[property]}, filtered_list)
    sorted_list = sorted(mapped_list, key=lambda item: item["value"])
    return sorted_list[:5]


def get_highest_assets_for_size_per_property(list_of_values_of_latest_investment: List[Dict], size: str, property: str) -> List[Dict]:
    filtered_list = filter(lambda item: item[size] is not None and item[size]
                           != 0 and item[property] is not None, list_of_values_of_latest_investment)
    mapped_list = map(lambda item: {
                      "label": item["asset"], "value": item[size] / item[property]}, filtered_list)
    sorted_list = sorted(
        mapped_list, key=lambda item: item["value"], reverse=True)
    return sorted_list[:5]


def get_3_lowest_industries_for_size_per_property(list_of_values_of_latest_investment: List[Dict], size: str, property: str) -> List[Dict]:
    industries = {}

    filtered_list = [
        item for item in list_of_values_of_latest_investment if item[property] is not None]

    for item in filtered_list:
        industry = item.get('industry')
        value = item[size] / item[property]
        if industry is not None:
            industries[industry] = industries.get(industry, 0) + value

    sorted_industries = sorted(industries.items(), key=lambda item: item[1])
    return [{"label": industry, "value": value} for industry, value in sorted_industries[:3]]


def get_3_highest_industries_for_size_per_property(list_of_values_of_latest_investment: List[Dict], size: str, property: str) -> List[Dict]:
    industries = {}

    filtered_list = [
        item for item in list_of_values_of_latest_investment if item[property] is not None]

    for item in filtered_list:
        value = item[size] / item[property]
        industry = item.get('industry')
        if industry is not None:
            industries[industry] = industries.get(industry, 0) + value

    sorted_industries = sorted(
        industries.items(), key=lambda item: item[1], reverse=True)
    return [{"label": industry, "value": value} for industry, value in sorted_industries[:3]]


def map_asset_types_in_company_value_industry_obj(asset_list: List[Dict]) -> List[Dict]:
    for asset in asset_list:
        if asset['assetType'] in dictionary_asset_types:
            asset['assetType'] = dictionary_asset_types[asset['assetType']]
    return asset_list


def map_countries_in_company_value_industry_obj(countries_list: List[Dict]) -> List[Dict]:
    for asset in countries_list:
        if asset['country'] in dictionary_countries:
            asset['country'] = dictionary_countries[asset['country']]
    return countries_list


def map_investment_types_in_company_value_industry_obj(investment_type_list: List[Dict]) -> List[Dict]:
    for asset in investment_type_list:
        if asset['investmentType'] in dictionary_investment_types:
            asset['investmentType'] = dictionary_investment_types[asset['investmentType']]
    return investment_type_list


def get_value_per_type(list_of_values_of_latest_investment: List[Dict], type_key: str, value_key: str) -> List[Dict]:
    count_map = {}
    total_sum = sum(item.get(value_key, 0)
                    for item in list_of_values_of_latest_investment)

    for item in list_of_values_of_latest_investment:
        value = item.get(type_key)
        if value:
            if value in count_map:
                count_map[value] += item.get(value_key, 0)
            else:
                count_map[value] = item.get(value_key, 0)

    result = []
    for name, count in count_map.items():
        value_sum = sum(asset.get(value_key, 0)
                        for asset in list_of_values_of_latest_investment if asset.get(type_key) == name)
        percentage = value_sum / total_sum if total_sum != 0 else 0
        result.append({"label": name, "value": value_sum,
                      "percentage": percentage})

    return result


def get_emission_last_years_for_scope(scope: str, sum_of_metrics):
    scope_value = None
    scope_value_nm1 = None
    scope_value_nm2 = None

    if scope == 'scope1':
        scope_value = sum_of_metrics['scope1']
        scope_value_nm1 = sum_of_metrics['scope1Nm1']
        scope_value_nm2 = sum_of_metrics['scope1Nm2']
    elif scope == 'scope2':
        scope_value = sum_of_metrics['scope2']
        scope_value_nm1 = sum_of_metrics['scope2Nm1']
        scope_value_nm2 = sum_of_metrics['scope2Nm2']
    elif scope == 'scope3':
        scope_value = sum_of_metrics['scope3']
        scope_value_nm1 = sum_of_metrics['scope3Nm1']
        scope_value_nm2 = sum_of_metrics['scope3Nm2']

    current_year = 2021
    return [
        {'value': scope_value, 'year': current_year},
        {'value': scope_value_nm1, 'year': current_year - 1},
        {'value': scope_value_nm2, 'year': current_year - 2}
    ]


def get_emission_last_years_for_total(sum_of_metrics: Dict[str, float]) -> List[Dict]:
    current_year = 2021
    return [
        {
            "scope1": sum_of_metrics["scope1"],
            "scope2": sum_of_metrics["scope2"],
            "scope3": sum_of_metrics["scope3"],
            "total": sum_of_metrics["total"],
            "year": current_year - i
        } for i in range(3)
    ]


def get_emission_intensity_per_property_for_total(asset_list: List[Dict], property_key: str) -> List[Dict]:
    current_year = asset_list[0]["investmentFundYear"]
    arr = []
    arr_nm1 = []
    arr_nm2 = []
    for asset in asset_list:
        if asset[property_key] is not None:
            arr.append(asset.get("total", 0) / asset[property_key])
            arr_nm1.append(asset.get("totalNm1", 0) / asset[property_key])
            arr_nm2.append(asset.get("totalNm2", 0) / asset[property_key])
    return [
        {"value": sum(arr), "year": current_year},
        {"value": sum(arr_nm1), "year": current_year - 1},
        {"value": sum(arr_nm2), "year": current_year - 2}
    ]


def get_sum_of_metrics(listOfvaluesOfLatestInvestment: List[Dict]) -> Dict:
    return {
        'scope1': sum(filter(lambda item: item['scope1'] is not None, map(lambda item: item['scope1'], listOfvaluesOfLatestInvestment))),
        'scope2': sum(filter(lambda item: item['scope2'] is not None, map(lambda item: item['scope2'], listOfvaluesOfLatestInvestment))),
        'scope3': sum(filter(lambda item: item['scope3'] is not None, map(lambda item: item['scope3'], listOfvaluesOfLatestInvestment))),
        'total': sum(filter(lambda item: item['total'] is not None, map(lambda item: item['total'], listOfvaluesOfLatestInvestment))),
        'scope1Nm1': sum(filter(lambda item: item['scope1Nm1'] is not None, map(lambda item: item['scope1Nm1'], listOfvaluesOfLatestInvestment))),
        'scope2Nm1': sum(filter(lambda item: item['scope2Nm1'] is not None, map(lambda item: item['scope2Nm1'], listOfvaluesOfLatestInvestment))),
        'scope3Nm1': sum(filter(lambda item: item['scope3Nm1'] is not None, map(lambda item: item['scope3Nm1'], listOfvaluesOfLatestInvestment))),
        'totalNm1': sum(filter(lambda item: item['totalNm1'] is not None, map(lambda item: item['totalNm1'], listOfvaluesOfLatestInvestment))),
        'scope1Nm2': sum(filter(lambda item: item['scope1Nm2'] is not None, map(lambda item: item['scope1Nm2'], listOfvaluesOfLatestInvestment))),
        'scope2Nm2': sum(filter(lambda item: item['scope2Nm2'] is not None, map(lambda item: item['scope2Nm2'], listOfvaluesOfLatestInvestment))),
        'scope3Nm2': sum(filter(lambda item: item['scope3Nm2'] is not None, map(lambda item: item['scope3Nm2'], listOfvaluesOfLatestInvestment))),
        'totalNm2': sum(filter(lambda item: item['totalNm2'] is not None, map(lambda item: item['totalNm2'], listOfvaluesOfLatestInvestment))),
    }


def get_values_for_scope1(fund_environmental: FundEnvironmentalResponse, list_values_latest_investment, sum_of_metrics):
    fund_environmental.CO2S1.bestAssetsPerEurInvested.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'amount_invested'
        ) or []
    )
    fund_environmental.CO2S1.bestIndustriesPerEurInvested.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'amount_invested'
        ) or []
    )
    fund_environmental.CO2S1.bestAssetsPerRevenue.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'revenue'
        ) or []
    )
    fund_environmental.CO2S1.bestIndustriesPerRevenue.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'revenue'
        ) or []
    )
    fund_environmental.CO2S1.worstAssetsPerEurInvested.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'amount_invested'
        ) or []
    )
    fund_environmental.CO2S1.worstIndustriesPerEurInvested.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'amount_invested'
        ) or []
    )
    fund_environmental.CO2S1.worstAssetsPerRevenue.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'revenue'
        ) or []
    )
    fund_environmental.CO2S1.worstIndustriesPerRevenue.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment,
            'scope1',
            'revenue'
        ) or []
    )
    fund_environmental.CO2S1.highestEmissionValue = max(
        item['scope1'] for item in list_values_latest_investment if item.get('scope1') is not None
    )
    fund_environmental.CO2S1.lowestEmissionValue = min(
        item['scope1'] for item in list_values_latest_investment if item.get('scope1') is not None
    )
    fund_environmental.CO2S1.emissionData = get_emission_last_years_for_scope(
        'scope1',
        sum_of_metrics
    )


def get_values_for_scope2(fund_environmental: FundEnvironmentalResponse, list_values_latest_investment, sum_of_metrics):
    fund_environmental.CO2S2.bestAssetsPerEurInvested.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'scope2', 'amount_invested') or []
    )
    fund_environmental.CO2S2.bestIndustriesPerEurInvested.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'scope2', 'amount_invested') or []
    )
    fund_environmental.CO2S2.bestAssetsPerRevenue.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'scope2', 'revenue') or []
    )
    fund_environmental.CO2S2.bestIndustriesPerRevenue.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'scope2', 'revenue') or []
    )
    fund_environmental.CO2S2.worstAssetsPerEurInvested.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'scope2', 'amount_invested') or []
    )
    fund_environmental.CO2S2.worstIndustriesPerEurInvested.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'scope2', 'amount_invested') or []
    )
    fund_environmental.CO2S2.worstAssetsPerRevenue.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'scope2', 'revenue') or []
    )
    fund_environmental.CO2S2.worstIndustriesPerRevenue.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'scope2', 'revenue') or []
    )
    fund_environmental.CO2S2.highestEmissionValue = max(
        item.scope2 for item in list_values_latest_investment if item.scope2 is not None
    )
    fund_environmental.CO2S2.lowestEmissionValue = min(
        item.scope2 for item in list_values_latest_investment if item.scope2 is not None
    )
    fund_environmental.CO2S2.emission_data = get_emission_last_years_for_scope(
        'scope2', sum_of_metrics)


def get_values_for_scope3(fund_environmental: FundEnvironmentalResponse, list_values_latest_investment, sum_of_metrics):
    fund_environmental.CO2S3.bestAssetsPerEurInvested.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'scope3', 'amount_invested') or []
    )
    fund_environmental.CO2S3.bestIndustriesPerEurInvested.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'scope3', 'amount_invested') or []
    )
    fund_environmental.CO2S3.bestAssetsPerRevenue.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'scope3', 'revenue') or []
    )
    fund_environmental.CO2S3.bestIndustriesPerRevenue.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'scope3', 'revenue') or []
    )
    fund_environmental.CO2S3.worstAssetsPerEurInvestedt.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'scope3', 'amount_invested') or []
    )
    fund_environmental.CO2S3.worstIndustriesPerEurInvested.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'scope3', 'amount_invested') or []
    )
    fund_environmental.CO2S3.worstAssetsPerRevenue.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'scope3', 'revenue') or []
    )
    fund_environmental.CO2S3.worstIndustriesPerRevenue.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'scope3', 'revenue') or []
    )
    fund_environmental.CO2S3.highestEmissionValue = max(
        item.scope3 for item in list_values_latest_investment if item.scope3 is not None
    )
    fund_environmental.CO2S3.lowestEmissionValue = min(
        item.scope3 for item in list_values_latest_investment if item.scope3 is not None
    )
    fund_environmental.CO2S3.emission_data = get_emission_last_years_for_scope(
        'scope3', sum_of_metrics)


def get_value_for_total(fund_environmental: FundEnvironmentalResponse, list_values_latest_investment, sum_of_metrics):
    fund_environmental.CO2T.bestAssetsPerEurInvested.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'total', 'amount_invested') or []
    )
    fund_environmental.CO2T.bestIndustriesPerEurInvested.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'total', 'amount_invested') or []
    )
    fund_environmental.CO2T.bestAssetsPerRevenue.extend(
        get_lowest_assets_for_size_per_property(
            list_values_latest_investment, 'total', 'revenue') or []
    )
    fund_environmental.CO2T.bestIndustriesPerRevenue.extend(
        get_3_lowest_industries_for_size_per_property(
            list_values_latest_investment, 'total', 'revenue') or []
    )
    fund_environmental.CO2T.worstAssetsPerEurInvestedt.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'total', 'amount_invested') or []
    )
    fund_environmental.CO2T.worstIndustriesPerEurInvested.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'total', 'amount_invested') or []
    )
    fund_environmental.CO2T.worstAssetsPerRevenue.extend(
        get_highest_assets_for_size_per_property(
            list_values_latest_investment, 'total', 'revenue') or []
    )
    fund_environmental.CO2T.worstIndustriesPerRevenue.extend(
        get_3_highest_industries_for_size_per_property(
            list_values_latest_investment, 'total', 'revenue') or []
    )

    fund_environmental.CO2T.co2_per_asset_type = get_value_per_type(
        map_asset_types_in_company_value_industry_obj(list_values_latest_investment), 'assetType', 'total')
    fund_environmental.CO2T.co2_per_country = get_value_per_type(
        map_countries_in_company_value_industry_obj(list_values_latest_investment), 'country', 'total')
    fund_environmental.CO2T.co2_per_industry = get_value_per_type(
        list_values_latest_investment, 'industry', 'total')
    fund_environmental.CO2T.total_emission_data = get_emission_last_years_for_total(
        list_values_latest_investment, sum_of_metrics)
    fund_environmental.CO2T.emissions_intensity_per_revenue = get_emission_intensity_per_property_for_total(
        list_values_latest_investment, 'revenue')
    fund_environmental.CO2T.emissions_intensity_per_eur_invested = get_emission_intensity_per_property_for_total(
        list_values_latest_investment, 'amount_invested')


def get_ranking_co2(event, asset_id, metric_name):
    try:
        metric_id = event.select("SELECT id from metric WHERE name = (:metric_name)", {
                                 'metric_name': metric_name}).val()
        if len(metric_id) == 0:
            raise QueryParameterMismatch(f"Metric {metric_name} not found")

        values = event.select("SELECT asset_id, value from asset_metric_value WHERE metric_id = (:metric_id) AND YEAR(date) = (:year)", {
                              'metric_id': metric_id, 'year': 2021}).list()
        if len(values) == 0:
            raise QueryParameterMismatch(
                f"Values for metric {metric_name} not found")

        # Sorting the values array and calculating the index of the specified asset_id
        values.sort(key=lambda x: x['value'])
        asset_rank = next((i for i, item in enumerate(values)
                          if item['asset_id'] == asset_id), None)

        return asset_rank
    except Exception as e:
        raise e


def get_emission_intensity_per_euro_invested_for_asset(event, asset_info, asset_id):
    values = event.select(
        "SELECT * FROM investment_detail WHERE asset_id =(:asset_id)", {'asset_id': asset_id}).list()

    eur_invested = sum(detail['nominal_amount'] for detail in values)

    return [
        {'year': 2019, 'value': asset_info['totalNm2'] / eur_invested},
        {'year': 2020, 'value': asset_info['totalNm1'] / eur_invested},
        {'year': 2021, 'value': asset_info['total'] / eur_invested}
    ]


def get_emission_data(asset_info):
    return [
        {
            'scope1': asset_info.get('scope1Nm2'),
            'scope2': asset_info.get('scope2Nm2'),
            'scope3': asset_info.get('scope3Nm2'),
            'total': asset_info.get('totalNm2'),
            'year': 2021 - 2
        },
        {
            'scope1': asset_info.get('scope1Nm1'),
            'scope2': asset_info.get('scope2Nm1'),
            'scope3': asset_info.get('scope3Nm1'),
            'total': asset_info.get('totalNm1'),
            'year': 2021 - 1
        },
        {
            'scope1': asset_info.get('scope1'),
            'scope2': asset_info.get('scope2'),
            'scope3': asset_info.get('scope3'),
            'total': asset_info.get('total'),
            'year': 2021
        }
    ]
    
    
def get_emission_intensity_per_revenue(asset_info: Dict[str, Optional[int]]):
    return [
        {
            'year': 2021 - 2,
            'value': asset_info.get('totalNm2') / asset_info.get('revenue') if asset_info.get('revenue') else None
        },
        {
            'year': 2021 - 1,
            'value': asset_info.get('totalNm1') / asset_info.get('revenue') if asset_info.get('revenue') else None
        },
        {
            'year': 2021,
            'value': asset_info.get('total') / asset_info.get('revenue') if asset_info.get('revenue') else None
        }
    ]