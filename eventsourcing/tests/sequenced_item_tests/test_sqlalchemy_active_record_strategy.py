from eventsourcing.infrastructure.sqlalchemy.activerecords import SQLAlchemyActiveRecordStrategy, \
    SqlIntegerSequencedItem, SqlTimestampSequencedItem, SqlSnapshot
from eventsourcing.infrastructure.sequenceditem import SequencedItem
from eventsourcing.tests.datastore_tests.test_sqlalchemy import SQLAlchemyDatastoreTestCase
from eventsourcing.tests.sequenced_item_tests.base import IntegerSequencedItemTestCase, \
    SimpleSequencedItemteratorTestCase, ThreadedSequencedItemIteratorTestCase, TimestampSequencedItemTestCase, \
    WithActiveRecordStrategies


def construct_integer_sequenced_active_record_strategy(datastore):
    return SQLAlchemyActiveRecordStrategy(
        active_record_class=SqlIntegerSequencedItem,
        sequenced_item_class=SequencedItem,
        session=datastore.db_session,
    )


def construct_timestamp_sequenced_active_record_strategy(datastore):
    return SQLAlchemyActiveRecordStrategy(
        active_record_class=SqlTimestampSequencedItem,
        sequenced_item_class=SequencedItem,
        session=datastore.db_session,
    )


def construct_snapshot_active_record_strategy(datastore):
    return SQLAlchemyActiveRecordStrategy(
        active_record_class=SqlSnapshot,
        sequenced_item_class=SequencedItem,
        session=datastore.db_session,
    )


class TestSQLAlchemyActiveRecordStrategyWithIntegerSequences(SQLAlchemyDatastoreTestCase,
                                                             IntegerSequencedItemTestCase):
    def construct_active_record_strategy(self):
        return construct_integer_sequenced_active_record_strategy(self.datastore)


class TestSQLAlchemyActiveRecordStrategyWithTimestampSequences(SQLAlchemyDatastoreTestCase,
                                                               TimestampSequencedItemTestCase):
    def construct_active_record_strategy(self):
        return construct_timestamp_sequenced_active_record_strategy(self.datastore)


class WithSQLAlchemyActiveRecordStrategies(WithActiveRecordStrategies, SQLAlchemyDatastoreTestCase):
    def construct_integer_sequenced_active_record_strategy(self):
        return construct_integer_sequenced_active_record_strategy(self.datastore)

    def construct_timestamp_sequenced_active_record_strategy(self):
        return construct_timestamp_sequenced_active_record_strategy(self.datastore)

    def construct_snapshot_active_record_strategy(self):
        return construct_snapshot_active_record_strategy(self.datastore)


class TestSimpleIteratorWithSQLAlchemy(WithSQLAlchemyActiveRecordStrategies,
                                       SimpleSequencedItemteratorTestCase):
    pass


class TestThreadedIteratorWithSQLAlchemy(WithSQLAlchemyActiveRecordStrategies,
                                         ThreadedSequencedItemIteratorTestCase):
    use_named_temporary_file = True
