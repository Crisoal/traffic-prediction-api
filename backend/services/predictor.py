import numpy as np
from keras.models import load_model
from geopy.distance import distance

# Load the pre-trained LSTM model
model = load_model('path/to/your/lstm_model.h5')

def predict_traffic(merged_data, latitude, longitude, intervals):
    # Extract the relevant feature vector for the given latitude and longitude
    feature_vector = merged_data[(merged_data['latitude_0'] == latitude) & (merged_data['longitude_0'] == longitude)].drop(columns=['timestamp', 'datetime', 'latitude', 'longitude'])
    
    if feature_vector.empty:
        raise ValueError("No data available for the specified location.")
    
    feature_vector = feature_vector.values[-1].reshape(1, 1, -1)

    # Generate predictions for the specified intervals
    predictions = {}
    for interval in intervals:
        pred = model.predict(feature_vector)
        predictions[f'{interval}_minutes'] = pred[0, 0]
        feature_vector = np.append(feature_vector[:, 1:, :], pred.reshape(1, 1, -1), axis=1)
    
    return predictions

def find_alternative_routes(merged_data, latitude, longitude, intervals):
    # Find the closest road segments that are not congested
    road_segments = merged_data[(merged_data['congestion_level'] < 0.5) & (merged_data['latitude'] != latitude) & (merged_data['longitude'] != longitude)]
    
    if road_segments.empty:
        raise ValueError("No alternative routes available.")
    
    road_segments['distance'] = road_segments.apply(lambda row: distance((latitude, longitude), (row['latitude'], row['longitude'])).meters, axis=1)
    road_segments = road_segments.sort_values('distance')
    
    alternative_routes = []
    for interval in intervals:
        closest_segment = road_segments.iloc[0]
        route_info = {
            'interval': interval,
            'latitude': closest_segment['latitude'],
            'longitude': closest_segment['longitude'],
            'congestion_level': closest_segment['congestion_level'],
            'road_length': closest_segment['road_length']
        }
        alternative_routes.append(route_info)
        road_segments = road_segments.iloc[1:]
    
    return alternative_routes
