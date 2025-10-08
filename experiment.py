import marimo

__generated_with = "0.16.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""## Download GTFS-RT protobufs""")
    return


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
def _(mo):
    mo.md(r"""## Download GTFS-static schedule""")
    return


@app.cell
def _(mo):
    mo.md(r"""## Import GTFS-static schedule into DuckDb""")
    return


@app.cell
def _(mo):
    mo.md(r"""## Import GTFS RT protobufs into DuckDB""")
    return


@app.cell
def _(tu_feed, vp_feed):
    # duckdb can only read dictionaries and/or JSON for now
    # there is an extension to read protobuf directly. Maybe I will try that if the performance is bad enough with dicts

    # it takes 13 seconds to run this cell :(  so maybe converting like this is too expensive
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
    # E001 - Not in POSIX time
    duckdb.query("SELECT * FROM trip_updates WHERE timestamp = 0")
    return


@app.cell
def _(mo):
    mo.md(r"""## Evaluate Rules""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E002 - stop_time_updates not strictly sorted
        SELECT * FROM
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E003 - GTFS-rt trip_id does not exist in GTFS data
        SELECT * FROM
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E004 - GTFS-rt route_id does not exist in GTFS data
        SELECT * FROM
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E006 - Missing required trip field for frequency-based exact_times = 0
        SELECT * FROM
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r""" """)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E009 - GTFS-rt stop_sequence isn't provided for trip that visits same stop_id more than once
        SELECT * FROM
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E010 - location_type not 0 in stops.txt
        SELECT * FROM
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- E011 - GTFS-rt stop_id does not exist in GTFS data
        SELECT * FROM
        """
    )
    return


if __name__ == "__main__":
    app.run()
