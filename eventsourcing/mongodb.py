from dataclasses import asdict, fields
from typing import Any, Optional, Sequence
from uuid import UUID

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    Notification,
    ProcessRecorder,
    StoredEvent,
)
from eventsourcing.utils import Environment
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError


class MongoDBAggregateRecorder(AggregateRecorder):

    def __init__(
        self,
        mongodb: Database,
        purpose: str = "events",
    ) -> None:
        self.stored_events: list[StoredEvent] = []
        self.collection = mongodb[purpose]
        self.collection.create_index(
            [("originator_id", 1), ("originator_version", 1)],
            unique=True,
        )

    def insert_events(
        self,
        stored_events: list[StoredEvent],
        **kwargs: Any,
    ) -> Optional[Sequence[int]]:
        notification_ids = []
        for event in stored_events:
            try:
                self.collection.insert_one(asdict(event))
            except DuplicateKeyError:
                event_dict = asdict(event)
                event_dict["originator_version"] += 1
                stored_events.append(StoredEvent(**event_dict))
            notification_ids.append(self.collection.count())
        return notification_ids

    def apply_params(
        self,
        query: dict[str, Any],
        gt: Optional[int] = None,
        lte: Optional[int] = None,
    ) -> None:
        if gt:
            query.update({"originator_version": {"$gt": gt}})
        if lte and "originator_version" in query:
            old_condition = query.pop("originator_version")
            query.update({
                "$and": [
                    {"originator_version": old_condition},
                    {"originator_version": {"$lte": lte}},
                ],
            })
        elif lte:
            query.update({"originator_version": {"$lte": lte}})

    def select_events(
        self,
        originator_id: UUID,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> list[StoredEvent]:
        query = {"originator_id": originator_id}
        self.apply_params(query, gt, lte)
        cursor = self.collection.find(query)

        if desc:
            cursor = cursor.sort([("originator_version", DESCENDING)])
        else:
            cursor = cursor.sort([("originator_version", ASCENDING)])
        if limit:
            cursor = cursor.limit(limit)

        result = []
        event_fields = [field.name for field in fields(StoredEvent)]
        for event in cursor:
            result.append(
                StoredEvent(
                    **{
                        event_key: event_value
                        for event_key, event_value in event.items()
                        if event_key in event_fields
                    },
                ),
            )

        return result


class MongoDBApplicationRecorder(
    ApplicationRecorder,
    MongoDBAggregateRecorder,
):

    def select_notifications(
        self,
        start: int,
        limit: int,
        stop: Optional[int] = None,
        topics: Sequence[str] = (),
    ) -> list[Notification]:
        cursor = self.collection.find().skip(start - 1).limit(limit)
        results = []
        for notification_id, obj in enumerate(cursor, start):
            if stop is not None and notification_id > stop - 1:
                break
            if topics and obj.get("topic") not in topics:
                continue
            notification = Notification(
                id=notification_id,
                originator_id=obj.get("originator_id"),
                originator_version=obj.get("originator_version"),
                topic=obj.get("topic"),
                state=obj.get("state"),
            )
            results.append(notification)

        return results

    def max_notification_id(self) -> int:
        return self.collection.count()


class MongoDBProcessRecorder(ProcessRecorder, MongoDBApplicationRecorder):

    def __init__(self, mongodb: Database) -> None:
        super().__init__(mongodb, purpose="tracking")

    def max_tracking_id(self, application_name: str) -> int:
        max_id_record = self.collection.find_one().sort(
            [("notification_id", DESCENDING)],
        ).limit(1)
        if max_id_record:
            return max_id_record.get("notification_id", 0)

        return 0


class Factory(InfrastructureFactory):
    MONGODB_NAME = "MONGODB_NAME"
    MONGODB_HOST = "MONGODB_HOST"
    MONGODB_PORT = "MONGODB_PORT"
    MONGODB_USER = "MONGODB_USER"
    MONGODB_PASSWORD = "MONGODB_PASSWORD"

    def __init__(self, env: Environment):
        super().__init__(env)
        mongodb_name = self.env.get(self.MONGODB_NAME)
        mongodb_host = self.env.get(self.MONGODB_HOST)
        mongodb_port = self.env.get(self.MONGODB_PORT)
        mongodb_user = self.env.get(self.MONGODB_USER)
        mongodb_password = self.env.get(self.MONGODB_PASSWORD)
        env_ok = all(
            [
                var is not None for var in (
                    mongodb_name,
                    mongodb_host,
                    mongodb_port,
                    mongodb_user,
                    mongodb_password,
                )
            ],
        )

        if not env_ok:
            raise EnvironmentError(
                "Some or all MongoDB environment variables not found",
            )

        self.mongodb_client = MongoClient(
            "mongodb://{0}:{1}@{2}:{3}/{4}".format(
                mongodb_user,
                mongodb_password,
                mongodb_host,
                mongodb_port,
                mongodb_name,
            ),
            tz_aware=True,
        )
        self.mongodb = self.mongodb_client[mongodb_name]

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        return MongoDBAggregateRecorder(self.mongodb, purpose)

    def application_recorder(self) -> ApplicationRecorder:
        return MongoDBApplicationRecorder(self.mongodb)

    def process_recorder(self) -> ProcessRecorder:
        return MongoDBProcessRecorder(self.mongodb)
