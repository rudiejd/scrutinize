from scrutinize.validators import validate_feeds
from pb.gtfs_realtime_pb2 import FeedMessage
import requests



def main():
    try:
        vehicle_positions = requests.get('https://cdn.mbta.com/realtime/VehiclePositions.pb')
        vehicle_positions.raise_for_status()
        trip_updates = requests.get('https://cdn.mbta.com/realtime/TripUpdates.pb')
        trip_updates.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error when pulling GTFS-RT {e}")
        exit()

    vp_feed = FeedMessage()
    vp_feed.ParseFromString(vehicle_positions.content)
    tu_feed = FeedMessage()
    tu_feed.ParseFromString(trip_updates.content)


    print(validate_feeds(vp_feed, tu_feed))


if __name__ == "__main__":
    main()
