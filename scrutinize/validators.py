from abc import ABC, abstractmethod
from pb.gtfs_realtime_pb2 import VehiclePosition, FeedMessage
from datetime import datetime

class Validator(ABC):
    @abstractmethod
    # returns failures as list of string. Empty list indicates success
    def validate(self, vp_feed: FeedMessage, tu_feed: FeedMessage) -> list[str]:
        ...

class PosixTimeValidator(Validator):
    def validate(vp_feed: FeedMessage, tu_feed: FeedMessage):
        failures = []
        for vp in vp_feed.entity:
            try:
                datetime.fromtimestamp(entity.vehicle.timestamp)
            except:
                return False
            return True

class PosixTimeValidator(VehiclePositionValidator):
    def failure(vp: VehiclePosition):
        return f"E001: entity.{vp.id}.vehicle.timestamp"
    def validate(entity: VehiclePosition):



def validate_feeds(vp_feed: FeedMessage, tu_feed: FeedMessage):
    failures = []
    for validator in Validator.__subclasses__():
        failures.append(validator.failures)
    return failures
