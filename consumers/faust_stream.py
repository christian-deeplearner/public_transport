"""Defines trends calculations for stations"""
import logging

import faust
import json
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("org.cta.chicago.stations.stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect-stations", value_type=Station)
out_topic = app.topic("org.cta.chicago.stations.table.v1", partitions=1)
table = app.Table(
   "stations.table",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def process(stations):
    async for station in stations:
        color = ""
        if station.red:
            color = "red"
        elif station.blue:
            color = "blue"
        elif station.green:
            color = "green"
        else:
            color = "red"

        transformed_statation = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=color
        )

        table[station.station_id] = transformed_statation
        logger.info(f'Station {station.station_id}: {json.dumps(asdict(transformed_statation), indent=2)}')

if __name__ == "__main__":
    app.main()
