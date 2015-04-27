# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import LowVoltage.testing as _tst
from LowVoltage.actions.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value


def _is_dict(d):
    return isinstance(d, dict)


def _is_str(s):
    return isinstance(s, basestring)


def _is_float(s):
    return isinstance(s, float)


def _is_int(s):
    return isinstance(s, (int, long))


def _is_list_of_dict(l):
    return isinstance(l, list) and all(_is_dict(e) for e in l)


def _is_list_of_str(l):
    return isinstance(l, list) and all(_is_str(e) for e in l)


def _is_list_of_float(l):
    return isinstance(l, list) and all(_is_float(e) for e in l)


class TableDescription(object):
    """
    `TableDescription <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html>`__.
    """

    def __init__(
        self,
        AttributeDefinitions=None,
        CreationDateTime=None,
        GlobalSecondaryIndexes=None,
        ItemCount=None,
        KeySchema=None,
        LocalSecondaryIndexes=None,
        ProvisionedThroughput=None,
        TableName=None,
        TableSizeBytes=None,
        TableStatus=None,
        **dummy
    ):
        self.__attribute_definitions = AttributeDefinitions
        self.__creation_date_time = CreationDateTime
        self.__global_secondary_indexes = GlobalSecondaryIndexes
        self.__item_count = ItemCount
        self.__key_schema = KeySchema
        self.__local_secondary_indexes = LocalSecondaryIndexes
        self.__provisioned_throughput = ProvisionedThroughput
        self.__table_name = TableName
        self.__table_size_bytes = TableSizeBytes
        self.__table_status = TableStatus

    @property
    def attribute_definitions(self):
        """
        :type: ``None`` or list of :class:`.AttributeDefinition`
        """
        if _is_list_of_dict(self.__attribute_definitions):
            return [AttributeDefinition(**d) for d in self.__attribute_definitions]

    @property
    def creation_date_time(self):
        """
        :type: ``None`` or :class:`~datetime.datetime`
        """
        if _is_float(self.__creation_date_time):
            return datetime.datetime.utcfromtimestamp(self.__creation_date_time)

    @property
    def global_secondary_indexes(self):
        """
        :type: ``None`` or list of :class:`.GlobalSecondaryIndexDescription`
        """
        if _is_list_of_dict(self.__global_secondary_indexes):
            return [GlobalSecondaryIndexDescription(**d) for d in self.__global_secondary_indexes]

    @property
    def item_count(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__item_count):
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: ``None`` or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def local_secondary_indexes(self):
        """
        :type: ``None`` or list of :class:`.LocalSecondaryIndexDescription`
        """
        if _is_list_of_dict(self.__local_secondary_indexes):
            return [LocalSecondaryIndexDescription(**d) for d in self.__local_secondary_indexes]

    @property
    def provisioned_throughput(self):
        """
        :type: ``None`` or :class:`.ProvisionedThroughputDescription`
        """
        if _is_dict(self.__provisioned_throughput):
            return ProvisionedThroughputDescription(**self.__provisioned_throughput)

    @property
    def table_name(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__table_name):
            return self.__table_name

    @property
    def table_size_bytes(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__table_size_bytes):
            return long(self.__table_size_bytes)

    @property
    def table_status(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__table_status):
            return self.__table_status


class TableDescriptionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = TableDescription()
        self.assertIsNone(r.attribute_definitions)
        self.assertIsNone(r.creation_date_time)
        self.assertIsNone(r.global_secondary_indexes)
        self.assertIsNone(r.item_count)
        self.assertIsNone(r.key_schema)
        self.assertIsNone(r.local_secondary_indexes)
        self.assertIsNone(r.provisioned_throughput)
        self.assertIsNone(r.table_name)
        self.assertIsNone(r.table_size_bytes)
        self.assertIsNone(r.table_status)

    def test_all_set(self):
        r = TableDescription(
            AttributeDefinitions=[{}],
            CreationDateTime=1430147859.5,
            GlobalSecondaryIndexes=[{}],
            ItemCount=1,
            KeySchema=[{}],
            LocalSecondaryIndexes=[{}],
            ProvisionedThroughput={},
            TableName="Aaa",
            TableSizeBytes=42,
            TableStatus="ACTIVE",
        )
        self.assertIsInstance(r.attribute_definitions[0], AttributeDefinition)
        self.assertEqual(r.creation_date_time, datetime.datetime(2015, 4, 27, 15, 17, 39, 500000))
        self.assertIsInstance(r.global_secondary_indexes[0], GlobalSecondaryIndexDescription)
        self.assertEqual(r.item_count, 1)
        self.assertIsInstance(r.key_schema[0], KeySchemaElement)
        self.assertIsInstance(r.local_secondary_indexes[0], LocalSecondaryIndexDescription)
        self.assertIsInstance(r.provisioned_throughput, ProvisionedThroughputDescription)
        self.assertEqual(r.table_name, "Aaa")
        self.assertEqual(r.table_size_bytes, 42)
        self.assertEqual(r.table_status, "ACTIVE")


class AttributeDefinition(object):
    """
    `AttributeDefinition <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeDefinition.html>`__.
    """

    def __init__(
        self,
        AttributeName=None,
        AttributeType=None,
        **dummy
    ):
        self.__attribute_name = AttributeName
        self.__attribute_type = AttributeType

    @property
    def attribute_name(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__attribute_name):
            return self.__attribute_name

    @property
    def attribute_type(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__attribute_type):
            return self.__attribute_type


class AttributeDefinitionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = AttributeDefinition()
        self.assertIsNone(r.attribute_name)
        self.assertIsNone(r.attribute_type)

    def test_all_set(self):
        r = AttributeDefinition(AttributeName="a", AttributeType="b")
        self.assertEqual(r.attribute_name, "a")
        self.assertEqual(r.attribute_type, "b")


class GlobalSecondaryIndexDescription(object):
    """
    `GlobalSecondaryIndexDescription <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GlobalSecondaryIndexDescription.html>`__.
    """

    def __init__(
        self,
        IndexName=None,
        IndexSizeBytes=None,
        IndexStatus=None,
        ItemCount=None,
        KeySchema=None,
        Projection=None,
        ProvisionedThroughput=None,
        **dummy
    ):
        self.__index_name = IndexName
        self.__index_size_bytes = IndexSizeBytes
        self.__index_status = IndexStatus
        self.__item_count = ItemCount
        self.__key_schema = KeySchema
        self.__projection = Projection
        self.__provisioned_throughput = ProvisionedThroughput

    @property
    def index_name(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__index_name):
            return self.__index_name

    @property
    def index_size_bytes(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__index_size_bytes):
            return long(self.__index_size_bytes)

    @property
    def index_status(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__index_status):
            return self.__index_status

    @property
    def item_count(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__item_count):
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: ``None`` or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def projection(self):
        """
        :type: ``None`` or :class:`.Projection`
        """
        if _is_dict(self.__projection):
            return Projection(**self.__projection)

    @property
    def provisioned_throughput(self):
        """
        :type: ``None`` or :class:`.ProvisionedThroughputDescription`
        """
        if _is_dict(self.__provisioned_throughput):
            return ProvisionedThroughputDescription(**self.__provisioned_throughput)


class GlobalSecondaryIndexDescriptionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = GlobalSecondaryIndexDescription()
        self.assertIsNone(r.index_name)
        self.assertIsNone(r.index_size_bytes)
        self.assertIsNone(r.index_status)
        self.assertIsNone(r.item_count)
        self.assertIsNone(r.key_schema)
        self.assertIsNone(r.projection)
        self.assertIsNone(r.provisioned_throughput)

    def test_all_set(self):
        r = GlobalSecondaryIndexDescription(
            IndexName="a",
            IndexSizeBytes=42,
            IndexStatus="ACTIVE",
            ItemCount=57,
            KeySchema=[{}],
            Projection={},
            ProvisionedThroughput={},
        )
        self.assertEqual(r.index_name, "a")
        self.assertEqual(r.index_size_bytes, 42)
        self.assertEqual(r.index_status, "ACTIVE")
        self.assertEqual(r.item_count, 57)
        self.assertIsInstance(r.key_schema[0], KeySchemaElement)
        self.assertIsInstance(r.projection, Projection)
        self.assertIsInstance(r.provisioned_throughput, ProvisionedThroughputDescription)


class Projection(object):
    """
    `Projection <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Projection.html>`__.
    """

    def __init__(
        self,
        NonKeyAttributes=None,
        ProjectionType=None,
        **dummy
    ):
        self.__non_key_attributes = NonKeyAttributes
        self.__projection_type = ProjectionType

    @property
    def non_key_attributes(self):
        """
        :type: ``None`` or list of string
        """
        if _is_list_of_str(self.__non_key_attributes):
            return self.__non_key_attributes

    @property
    def projection_type(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__projection_type):
            return self.__projection_type


class ProjectionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = Projection()
        self.assertIsNone(r.non_key_attributes)
        self.assertIsNone(r.projection_type)

    def test_all_set(self):
        r = Projection(NonKeyAttributes=["a"], ProjectionType="b")
        self.assertEqual(r.non_key_attributes, ["a"])
        self.assertEqual(r.projection_type, "b")


class ProvisionedThroughputDescription(object):
    """
    `ProvisionedThroughputDescription <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ProvisionedThroughputDescription.html>`__.
    """

    def __init__(
        self,
        LastDecreaseDateTime=None,
        LastIncreaseDateTime=None,
        NumberOfDecreasesToday=None,
        ReadCapacityUnits=None,
        WriteCapacityUnits=None,
        **dummy
    ):
        self.__last_decrease_date_time = LastDecreaseDateTime
        self.__last_increase_date_time = LastIncreaseDateTime
        self.__number_of_decreases_today = NumberOfDecreasesToday
        self.__read_capacity_units = ReadCapacityUnits
        self.__write_capacity_units = WriteCapacityUnits

    @property
    def last_decrease_date_time(self):
        """
        :type: ``None`` or :class:`~datetime.datetime`
        """
        if _is_float(self.__last_decrease_date_time):
            return datetime.datetime.utcfromtimestamp(self.__last_decrease_date_time)

    @property
    def last_increase_date_time(self):
        """
        :type: ``None`` or :class:`~datetime.datetime`
        """
        if _is_float(self.__last_increase_date_time):
            return datetime.datetime.utcfromtimestamp(self.__last_increase_date_time)

    @property
    def number_of_decreases_today(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__number_of_decreases_today):
            return long(self.__number_of_decreases_today)

    @property
    def read_capacity_units(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__read_capacity_units):
            return long(self.__read_capacity_units)

    @property
    def write_capacity_units(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__write_capacity_units):
            return long(self.__write_capacity_units)


class ProvisionedThroughputDescriptionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = ProvisionedThroughputDescription()
        self.assertIsNone(r.last_decrease_date_time)
        self.assertIsNone(r.last_increase_date_time)
        self.assertIsNone(r.number_of_decreases_today)
        self.assertIsNone(r.read_capacity_units)
        self.assertIsNone(r.write_capacity_units)

    def test_all_set(self):
        r = ProvisionedThroughputDescription(
            LastDecreaseDateTime=1430148376.2,
            LastIncreaseDateTime=1430148384.2,
            NumberOfDecreasesToday=4,
            ReadCapacityUnits=5,
            WriteCapacityUnits=6,
        )
        self.assertEqual(r.last_decrease_date_time, datetime.datetime(2015, 4, 27, 15, 26, 16, 200000))
        self.assertEqual(r.last_increase_date_time, datetime.datetime(2015, 4, 27, 15, 26, 24, 200000))
        self.assertEqual(r.number_of_decreases_today, 4)
        self.assertEqual(r.read_capacity_units, 5)
        self.assertEqual(r.write_capacity_units, 6)


class KeySchemaElement(object):
    """
    `KeySchemaElement <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeySchemaElement.html>`__.
    """

    def __init__(
        self,
        AttributeName=None,
        KeyType=None,
        **dummy
    ):
        self.__attribute_name = AttributeName
        self.__key_type = KeyType

    @property
    def attribute_name(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__attribute_name):
            return self.__attribute_name

    @property
    def key_type(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__key_type):
            return self.__key_type


class KeySchemaElementUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = KeySchemaElement()
        self.assertIsNone(r.attribute_name)
        self.assertIsNone(r.key_type)

    def test_all_set(self):
        r = KeySchemaElement(AttributeName="a", KeyType="b")
        self.assertEqual(r.attribute_name, "a")
        self.assertEqual(r.key_type, "b")


class LocalSecondaryIndexDescription(object):
    """
    `LocalSecondaryIndexDescription <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_LocalSecondaryIndexDescription.html>`__.
    """

    def __init__(
        self,
        IndexName=None,
        IndexSizeBytes=None,
        ItemCount=None,
        KeySchema=None,
        Projection=None,
        **dummy
    ):
        self.__index_name = IndexName
        self.__index_size_bytes = IndexSizeBytes
        self.__item_count = ItemCount
        self.__key_schema = KeySchema
        self.__projection = Projection

    @property
    def index_name(self):
        """
        :type: ``None`` or string
        """
        if _is_str(self.__index_name):
            return self.__index_name

    @property
    def index_size_bytes(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__index_size_bytes):
            return long(self.__index_size_bytes)

    @property
    def item_count(self):
        """
        :type: ``None`` or long
        """
        if _is_int(self.__item_count):
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: ``None`` or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def projection(self):
        """
        :type: ``None`` or :class:`.Projection`
        """
        if _is_dict(self.__projection):
            return Projection(**self.__projection)


class LocalSecondaryIndexDescriptionUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = LocalSecondaryIndexDescription()
        self.assertIsNone(r.index_name)
        self.assertIsNone(r.index_size_bytes)
        self.assertIsNone(r.item_count)
        self.assertIsNone(r.key_schema)
        self.assertIsNone(r.projection)

    def test_all_set(self):
        r = LocalSecondaryIndexDescription(
            IndexName="a",
            IndexSizeBytes=42,
            ItemCount=57,
            KeySchema=[{}],
            Projection={},
        )
        self.assertEqual(r.index_name, "a")
        self.assertEqual(r.index_size_bytes, 42)
        self.assertEqual(r.item_count, 57)
        self.assertIsInstance(r.key_schema[0], KeySchemaElement)
        self.assertIsInstance(r.projection, Projection)


class ConsumedCapacity(object):
    """
    `ConsumedCapacity <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ConsumedCapacity.html>`__.
    """

    def __init__(
        self,
        CapacityUnits=None,
        GlobalSecondaryIndexes=None,
        LocalSecondaryIndexes=None,
        Table=None,
        TableName=None,
        **dummy
    ):
        self.__capacity_units = CapacityUnits
        self.__global_secondary_indexes = GlobalSecondaryIndexes
        self.__local_secondary_indexes = LocalSecondaryIndexes
        self.__table = Table
        self.__table_name = TableName

    @property
    def capacity_units(self):
        """
        The total capacity units consumed by the request.

        :type: ``None`` or float
        """
        if _is_float(self.__capacity_units):
            return float(self.__capacity_units)

    @property
    def global_secondary_indexes(self):
        """
        The capacity consumed on GSIs.

        :type: ``None`` or dict of string (index name) to :class:`.Capacity`
        """
        if _is_dict(self.__global_secondary_indexes):
            return {n: Capacity(**v) for n, v in self.__global_secondary_indexes.iteritems()}

    @property
    def local_secondary_indexes(self):
        """
        The capacity consumed on LSIs.

        :type: ``None`` or dict of string (index name) to :class:`.Capacity`
        """
        if _is_dict(self.__local_secondary_indexes):
            return {n: Capacity(**v) for n, v in self.__local_secondary_indexes.iteritems()}

    @property
    def table(self):
        """
        The capacity consumed on the table itself.

        :type: ``None`` or :class:`.Capacity`
        """
        if _is_dict(self.__table):
            return Capacity(**self.__table)

    @property
    def table_name(self):
        """
        The name of the table.

        :type: ``None`` or string
        """
        if _is_str(self.__table_name):
            return self.__table_name


class ConsumedCapacityUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = ConsumedCapacity()
        self.assertIsNone(r.capacity_units)
        self.assertIsNone(r.global_secondary_indexes)
        self.assertIsNone(r.local_secondary_indexes)
        self.assertIsNone(r.table)
        self.assertIsNone(r.table_name)

    def test_all_set(self):
        r = ConsumedCapacity(
            CapacityUnits=4.,
            GlobalSecondaryIndexes={"a": {}},
            LocalSecondaryIndexes={"b": {}},
            Table={},
            TableName="A",
        )
        self.assertEqual(r.capacity_units, 4.)
        self.assertIsInstance(r.global_secondary_indexes["a"], Capacity)
        self.assertIsInstance(r.local_secondary_indexes["b"], Capacity)
        self.assertIsInstance(r.table, Capacity)
        self.assertEqual(r.table_name, "A")


class Capacity(object):
    """
    `Capacity <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Capacity.html>`__.
    """

    def __init__(
        self,
        CapacityUnits=None,
        **dummy
    ):
        self.__capacity_units = CapacityUnits

    @property
    def capacity_units(self):
        """
        Actual units of consumed capacity.

        :type: ``None`` or float
        """
        if _is_float(self.__capacity_units):
            return float(self.__capacity_units)


class CapacityUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = Capacity()
        self.assertIsNone(r.capacity_units)

    def test_all_set(self):
        r = Capacity(CapacityUnits=4.)
        self.assertEqual(r.capacity_units, 4.)


class ItemCollectionMetrics(object):
    """
    `ItemCollectionMetrics <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ItemCollectionMetrics.html>`__.
    """

    def __init__(
        self,
        ItemCollectionKey=None,
        SizeEstimateRangeGB=None,
        **dummy
    ):
        self.__item_collection_key = ItemCollectionKey
        self.__size_estimate_range_gb = SizeEstimateRangeGB

    @property
    def item_collection_key(self):
        """
        Hash key of the collection whose size is estimated.

        :type: ``None`` or dict
        """
        if _is_dict(self.__item_collection_key):
            return _convert_db_to_dict(self.__item_collection_key)

    @property
    def size_estimate_range_gb(self):
        """
        Range of sizes of the collection in GB.

        :type: ``None`` or list of two float
        """
        if _is_list_of_float(self.__size_estimate_range_gb):
            return [float(e) for e in self.__size_estimate_range_gb]


class ItemCollectionMetricsUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = ItemCollectionMetrics()
        self.assertIsNone(r.item_collection_key)
        self.assertIsNone(r.size_estimate_range_gb)

    def test_all_set(self):
        r = ItemCollectionMetrics(ItemCollectionKey={"h": {"S": "a"}}, SizeEstimateRangeGB=[0., 1.])
        self.assertEqual(r.item_collection_key, {"h": "a"})
        self.assertEqual(r.size_estimate_range_gb, [0., 1.])
