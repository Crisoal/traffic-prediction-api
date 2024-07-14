from flask import Flask, jsonify, request, render_template
from models.traffic_data import get_historical_traffic_data, get_weather_data
from models.osm_data import get_osm_data
from services.preprocessor import preprocess_historical_data, preprocess_weather_data, preprocess_osm_data, merge_data
from services.predictor import predict_traffic, find_alternative_routes

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/current_traffic', methods=['GET'])
def current_traffic():
    try:
        traffic_data = get_historical_traffic_data(realtime=True)
        weather_data = get_weather_data()
        osm_data = get_osm_data()

        preprocessed_traffic = preprocess_historical_data(traffic_data)
        preprocessed_weather = preprocess_weather_data(weather_data)
        preprocessed_osm = preprocess_osm_data(osm_data)

        merged_data = merge_data(preprocessed_traffic, preprocessed_weather, preprocessed_osm)
        current_data = merged_data.iloc[-1]  # Treat the last stored data as real-time data

        return jsonify(current_data.to_dict())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/traffic_forecast', methods=['POST'])
def traffic_forecast():
    try:
        req_data = request.get_json()
        latitude = req_data.get('latitude')
        longitude = req_data.get('longitude')
        forecast_intervals = req_data.get('intervals', [15, 30, 45, 60, 120, 180])

        traffic_data = get_historical_traffic_data()
        weather_data = get_weather_data()
        osm_data = get_osm_data()

        preprocessed_traffic = preprocess_historical_data(traffic_data)
        preprocessed_weather = preprocess_weather_data(weather_data)
        preprocessed_osm = preprocess_osm_data(osm_data)

        merged_data = merge_data(preprocessed_traffic, preprocessed_weather, preprocessed_osm)

        predictions = predict_traffic(merged_data, latitude, longitude, forecast_intervals)

        return jsonify(predictions)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alternative_routes', methods=['POST'])
def alternative_routes():
    try:
        req_data = request.get_json()
        latitude = req_data.get('latitude')
        longitude = req_data.get('longitude')
        intervals = req_data.get('intervals', [15, 30, 45, 60, 120, 180])

        traffic_data = get_historical_traffic_data()
        weather_data = get_weather_data()
        osm_data = get_osm_data()

        preprocessed_traffic = preprocess_historical_data(traffic_data)
        preprocessed_weather = preprocess_weather_data(weather_data)
        preprocessed_osm = preprocess_osm_data(osm_data)

        merged_data = merge_data(preprocessed_traffic, preprocessed_weather, preprocessed_osm)

        alt_routes = find_alternative_routes(merged_data, latitude, longitude, intervals)

        return jsonify(alt_routes)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
