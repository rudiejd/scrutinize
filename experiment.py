import marimo

__generated_with = "0.16.3"
app = marimo.App(width="medium")


@app.cell
def _():
    from scrutinize.pb.gtfs_realtime_pb2 import FeedMessage, VehiclePosition, TripUpdate
    import requests

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
    return tu_feed, vp_feed


@app.cell
def _(tu_feed, vp_feed):
    # duckdb can only read dictionaries and/or JSON for now
    # there is an extension to read protobuf directly. Maybe I will try that if the performance is bad enough with dicts
    from google.protobuf import json_format
    import polars as pl
    import duckdb

    vp_dict = json_format.MessageToDict(vp_feed)
    tu_dict = json_format.MessageToDict(tu_feed)


    duckdb.query("CREATE OR REPLACE TABLE vehicle_positions AS (SELECT * FROM (SELECT unnest($data.entity, recursive := true)))", params={"data": vp_dict})
    duckdb.query("CREATE OR REPLACE TABLE trip_updates AS (SELECT * FROM (SELECT unnest($data.entity, recursive := true)))", params={"data": tu_dict})

    return (duckdb,)


@app.cell
def _(duckdb):
    duckdb.query("SELECT * FROM trip_updates WHERE timestamp = 0")
    return


if __name__ == "__main__":
    app.run()
