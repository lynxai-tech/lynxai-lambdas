def query_metric_value(event, asset_name, metric_name, year):
    return event.select("""SELECT amv.value FROM `schema`.asset_metric_value AS amv
                            JOIN `schema`.asset AS a ON amv.asset_id = a.id
                            JOIN `schema`.metric AS m ON amv.metric_id = m.id
                            WHERE a.name = assetName AND m.name = metricName 
                            AND YEAR(amv.date) = correctYear;""",
                        {
                            'asset_name': asset_name,
                            'metric_name': metric_name,
                            'year': year
                        }).val()
