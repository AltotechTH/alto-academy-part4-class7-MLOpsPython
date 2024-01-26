if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
from utils.preprocess_data import LoadForecastFeatureEngineer


@transformer
def transform(data, *args, **kwargs):
    
    col_list = ['pdrybulb_temperature', 'dewpoint_temperature', 'feels_like', 'humidity', 'phrase', 'wind_direction', 'wind_speed', 'uv_index', 'msl_pressure']
    # Feature engineering
    feature_engineer = LoadForecastFeatureEngineer(data_resolution='30min', forecast_horizon=192)

    df = feature_engineer.add_lagged_feature(
        data,
        col_list,
        48
    )

    return df
