import pickle
from flask import Flask, jsonify, request
import numpy as np
import json

app = Flask(__name__)

# Load the trained scikit-learn models stored in pickle format
with open('travelModel.pkl', 'rb') as f:
    modelo_tiempo_viaje = pickle.load(f)

with open('deliveryModel.pkl', 'rb') as f:
    modelo_tiempo_entrega = pickle.load(f)

with open('le.pkl', 'rb') as f:
    labelEncoder = pickle.load(f)



# Endpoint for route prediction model
# Input is a json object with attribute time
@app.route('/predict_eta', methods=['POST'])
def predict_eta():
    # Get the JSON data from the request body
    data = np.array(request.get_json()['time'],dtype = float) # Convert the JSON data into a numpy array --> texto que tengo que pasarlo a un float
    # Use the loaded model to make a prediction
    prediccion = modelo_tiempo_viaje.predict(data.reshape(1, -1))[0] # Reshape the data into a 2D array before making the prediction





    # Return the prediction as a JSON response
    return jsonify({'prediction': prediccion[0]})

# Endpoint for load delivery endpoint.
# Input is a json object with attributes truckId and time
@app.route('/predict_delivery', methods=['POST'])
def predict_delivery():
    # Get the JSON data from the request body
    truckId = np.array(request.get_json()['truckId'])
    time = np.array(request.get_json()['time'], dtype=float)

    input_features = np.array([truckId, time[0]]).reshape(1, -1)
    # Use the loaded model to make a prediction
    prediccion = modelo_tiempo_entrega.predict(input_features)[0]
    
    # Return the prediction as a JSON response  
    return jsonify({'prediction': prediccion[0]})


if __name__ == '__main__':
    app.run(debug=True, port =7777, host='0.0.0.0')