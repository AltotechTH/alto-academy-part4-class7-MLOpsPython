from flask import Flask, request, jsonify
import pycaret
import pandas as pd

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
    data = pd.DataFrame([message])

    # Make the prediction
    prediction = pycaret.predict_model(model, data)

    # Return the prediction
    return jsonify({'prediction': prediction})

if __name__ == '__main__':
    app.run(debug=True)