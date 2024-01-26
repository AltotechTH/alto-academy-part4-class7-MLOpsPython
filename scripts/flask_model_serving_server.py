from flask import Flask, request, jsonify
import pycaret
import pandas as pd

from preprocess_data import LoadForecastFeatureEngineer

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    # Get the message
    message = request.get_json()

    # Get the model name from the message
    model_name = message.get('model_name')

    # Load the Pycaret model
    model = pycaret.load_model(model_name)

    # Preprocess the message for model inference
    data = pd.DataFrame([message.get('data')])
    feature_engineer = LoadForecastFeatureEngineer(
        data_resolution='30min', 
        forecast_horizon=192
    )

    col_list = ['drybulb_temperature', 'dewpoint_temperature', 'feels_like', 'humidity', 'phrase', 'wind_direction', 'wind_speed', 'uv_index', 'msl_pressure']
    data = feature_engineer.add_lagged_feature(
        data,
        col_list,
        48
    )

    # Make the prediction
    prediction = pycaret.predict_model(model, data)

    # Return the prediction
    return jsonify({'prediction': prediction})

if __name__ == '__main__':
    app.run(debug=True)