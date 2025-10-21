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
    return requests, trip_updates, tu_feed, vehicle_positions, vp_feed


@app.cell
def _(mo):
    mo.md(r"""## Download GTFS-static schedule""")
    return


@app.cell
def _(duckdb, requests):
    from io import BytesIO, StringIO
    from zipfile import ZipFile

    static = requests.get("https://cdn.mbta.com/MBTA_GTFS.zip")

    with ZipFile(BytesIO(static.content)) as thezip:
        thezip.extractall("build/")

    duckdb.query("CREATE OR REPLACE TABLE trips AS SELECT * FROM read_csv('build/trips.txt', sample_size=-1)")
    duckdb.query("CREATE OR REPLACE TABLE routes AS SELECT * FROM read_csv('build/routes.txt', sample_size=-1)")
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
    return duckdb, tu_dict


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Evaluate Rules
    All of the below queries return rows for which the rule is not satisfied. If there are no rows, the rule passes.

    This covers everything except for alerts right now
    """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### E001 Not in POSIX time""")
    return


@app.cell
def _(mo, trip_updates):
    # E001 - Not in POSIX time
    _df = mo.sql(
        f"""
        -- E001 - Not in POSIX time
        -- trip_update.timestamp
        -- stop_time_update.arrival.time / stop_time_update.departure.time
        SELECT 
            TO_TIMESTAMP(timestamp::DOUBLE) tu_timestamp,
            -- vehicle ID
            id,
            tripId
        FROM trip_updates
        WHERE  

         -- non-posix STU date
         LENGTH(LIST_FILTER(stopTimeUpdate, 
            	LAMBDA stu : 
                    stu.scheduleRelationship != 'SKIPPED' AND
            		(DATE(TO_TIMESTAMP(stu.departure.time::DOUBLE)) != CURRENT_DATE() OR
            		DATE(TO_TIMESTAMP(stu.arrival.time::DOUBLE)) != CURRENT_DATE())
             )) > 0 OR
          -- non-posix trip_update.timestamp 
         DATE(tu_timestamp) != CURRENT_DATE()
        """
    )
    return


@app.cell
def _(mo, tu_dict):
    _df = mo.sql(
        f"""
        -- header.timestamp
        SELECT 'header.timestamp'
        WHERE DATE(TO_TIMESTAMP({tu_dict['header']['timestamp']}::DOUBLE)) != CURRENT_DATE()
        """
    )
    return


@app.cell
def _(mo, vehicle_positions):
    _df = mo.sql(
        f"""
        -- vehicle_position.timestamp
        SELECT 
            TO_TIMESTAMP(timestamp::DOUBLE) vp_timestamp,
            id,
            tripId
        FROM vehicle_positions
        WHERE 
            DATE(vp_timestamp) != CURRENT_DATE()
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### E002 - stop_time_updates not strictly sorted""")
    return


@app.cell
def _(mo, trip_updates):
    _df = mo.sql(
        f"""
        SELECT 
        	id,
        	tripId,
            TO_TIMESTAMP(timestamp::DOUBLE) tu_timestamp,
            LIST_TRANSFORM(stopTimeUpdate, LAMBDA stu: stu.stopSequence)
        FROM trip_updates
        WHERE  
            -- https://github.com/duckdb/duckdb/discussions/16247
            -- must be sorted
            LIST_ZIP(
            	LIST_TRANSFORM(stopTimeUpdate, LAMBDA stu: stu.stopSequence), 
            	stopTimeUpdate)
                .LIST_SORT()
                .LIST_TRANSFORM(x -> x[2]) != stopTimeUpdate OR
            -- no duplicates (strictly increasing)
            LENGTH(LIST_DISTINCT(LIST_TRANSFORM(stopTimeUpdate, LAMBDA stu : stu.stopSequence))) != LENGTH(stopTimeUpdate)
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### E003 - GTFS-rt trip_id does not exist in GTFS data""")
    return


@app.cell
def _(mo, trips, vehicle_positions):
    _df = mo.sql(
        f"""
        -- vehicle positions
        SELECT *, TO_TIMESTAMP(timestamp::DOUBLE) FROM vehicle_positions
        LEFT JOIN trips t ON tripId = t.trip_id
        WHERE t.trip_id IS NULL
        AND scheduleRelationship = 'SCHEDULED'
        """
    )
    return


@app.cell
def _(mo, trip_updates, trips):
    _df = mo.sql(
        f"""
        -- trip updates
        SELECT *, TO_TIMESTAMP(timestamp::DOUBLE) FROM trip_updates
        LEFT JOIN trips t ON tripId = t.trip_id
        WHERE t.trip_id IS NULL
        AND scheduleRelationship = 'SCHEDULED'
        """
    )
    return


@app.cell
def _(mo, routes, trip_updates, vehicle_positions):
    _df = mo.sql(
        f"""
        -- E004 - GTFS-rt route_id does not exist in GTFS data
        SELECT id_1 vehicleId, tripId, routeId
        FROM trip_updates tu
        LEFT JOIN routes r ON tu.routeId = r.route_id
        WHERE r.route_id IS NULL
        UNION ALL
        SELECT id AS vehicleId, tripId, routeId
        FROM vehicle_positions vp
        LEFT JOIN routes r ON vp.routeId = r.route_id
        WHERE r.route_id IS NULL;
        """
    )
    return


@app.cell
def _(mo, vehicle_positions):
    _df = mo.sql(
        f"""
        -- E006 - Missing required trip field for frequency-based exact_times = 0
        SELECT DISTINCT * from vehicle_positions
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
