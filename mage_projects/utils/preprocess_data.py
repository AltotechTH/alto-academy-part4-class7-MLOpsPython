import pandas as pd
from datetime import datetime, timedelta


class LoadForecastFeatureEngineer:
    """
    A class to feature engineering for load forecasting application.
    
    This class provides methods to add and calculate various time-based features 
    and lagged features to a pandas DataFrame, which are commonly used in load forecasting.

    Attributes:
        data_resolution (int): The resolution of the data in minutes.
        forecast_horizon (int): The forecast horizon in hours.
    """

    def __init__(self, data_resolution, forecast_horizon):
        """
        Initializes the LoadForecastFeatureEngineer with data resolution and forecast horizon.

        Args:
            data_resolution (int): The resolution of the data in minutes.
            forecast_horizon (int): The forecast horizon in hours.
        """
        self.data_resolution = data_resolution
        self.forcast_horizon = forecast_horizon

    def add_timebased_feature(self, df):
        """
        Add basic time-based features to the DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to which time-based features will be added.

        Returns:
            pandas.DataFrame: The DataFrame with added time-based features.
        """
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['day_of_month'] = df.index.day
        df['month'] = df.index.month
        df['year'] = df.index.year

        return df

    def _cal_timebased_avg_feature(self, row, data, days):
        """
        Calculate the average of a feature over a specified time period in the past.

        Args:
            row (pandas.Series): A row of the DataFrame for which the feature is calculated.
            data (pandas.DataFrame): The DataFrame containing the historical data.
            days (int): The number of days in the past to calculate the average.

        Returns:
            float: The calculated average feature value.
        """
        end_date = row['datetime']
        start_date = end_date - pd.Timedelta(days=days)
        filtered_data = data[(data['datetime'] >= start_date) & (data['datetime'] < end_date)]
        avg_feature = filtered_data[filtered_data['time'] == row['time']]['cooling_rate'].mean()
        
        return avg_feature

    def add_timebased_avg_feature(self, df, days):
        """
        Add a column to the DataFrame that represents the average of a feature over a specified time period.

        Args:
            df (pandas.DataFrame): The DataFrame to which the feature will be added.
            days (int): The number of days in the past to calculate the average.

        Returns:
            pandas.DataFrame: The DataFrame with the added average feature.
        """
        if 'datetime' in df.index.names:
            df = df.reset_index()
        df['time'] = df['datetime'].dt.time
        df[f'timebase_avg_{days}_days'] = df.apply(lambda row: self._cal_timebased_avg_feature(row, df, days), axis=1)

        df = df.dropna()

        return df
    
    def add_lagged_feature(self, df, col_list, lag):
        """
        Add lagged features to the DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to which lagged features will be added.
            col_list (list): List of column names for which lagged features are to be created.
            lag (int): The lag period in terms of number of rows.

        Returns:
            pandas.DataFrame: The DataFrame with added lagged features.
        """
        if 'datetime' in df.index.names:
            df = df.reset_index()
        for col in col_list:
            df[f'lagged{lag}_{col}'] = df[col].shift(lag)
        
        df = df.dropna()

        return df
    
    def preprocess_data_for_model_training(self,
                                        df, 
                                        datetime_column_name, 
                                        wetbulb_temperature_column_name,
                                        drybulb_temperature_column_name,
                                        humidity_column_name, 
                                        cooling_load_column_name, 
                                        data_resolution, 
                                        forecast_horizon):
        # Reformat data
        if wetbulb_temperature_column_name:

            df = df.rename(columns={cooling_load_column_name: "cooling_rate",
                                    wetbulb_temperature_column_name: "",
                                    drybulb_temperature_column_name: "outdoor_weather_station__drybulb_temperature",
                                    humidity_column_name: "outdoor_weather_station__relative_humidity", 
                                    datetime_column_name: "datetime"})
            df = df[['datetime', 'cooling_rate', 'outdoor_weather_station__drybulb_temperature', 'outdoor_weather_station__relative_humidity']]
        else:
            df = df.rename(columns={cooling_load_column_name: "cooling_rate",
                                    drybulb_temperature_column_name: "drybulb_temperature",
                                    humidity_column_name: "humidity", 
                                    datetime_column_name: "datetime"})
            df = df[['datetime', 'cooling_rate', 'drybulb_temperature', 'humidity']]

        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime')
        df = df.resample(data_resolution).mean()

        return df