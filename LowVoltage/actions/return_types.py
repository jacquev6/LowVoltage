# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

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
        :type: None or list of :class:`.AttributeDefinition`
        """
        if _is_list_of_dict(self.__attribute_definitions):  # pragma no branch (Defensive code)
            return [AttributeDefinition(**d) for d in self.__attribute_definitions]

    @property
    def creation_date_time(self):
        """
        :type: None or :class:`~datetime.datetime`
        """
        if _is_float(self.__creation_date_time):  # pragma no branch (Defensive code)
            return datetime.datetime.utcfromtimestamp(self.__creation_date_time)

    @property
    def global_secondary_indexes(self):
        """
        :type: None or list of :class:`.GlobalSecondaryIndexDescription`
        """
        if _is_list_of_dict(self.__global_secondary_indexes):  # pragma no branch (Defensive code)
            return [GlobalSecondaryIndexDescription(**d) for d in self.__global_secondary_indexes]

    @property
    def item_count(self):
        """
        :type: None or long
        """
        if _is_int(self.__item_count):  # pragma no branch (Defensive code)
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: None or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):  # pragma no branch (Defensive code)
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def local_secondary_indexes(self):
        """
        :type: None or list of :class:`.LocalSecondaryIndexDescription`
        """
        if _is_list_of_dict(self.__local_secondary_indexes):  # pragma no branch (Defensive code)
            return [LocalSecondaryIndexDescription(**d) for d in self.__local_secondary_indexes]

    @property
    def provisioned_throughput(self):
        """
        :type: None or :class:`.ProvisionedThroughputDescription`
        """
        if _is_dict(self.__provisioned_throughput):  # pragma no branch (Defensive code)
            return ProvisionedThroughputDescription(**self.__provisioned_throughput)

    @property
    def table_name(self):
        """
        :type: None or string
        """
        if _is_str(self.__table_name):  # pragma no branch (Defensive code)
            return self.__table_name

    @property
    def table_size_bytes(self):
        """
        :type: None or long
        """
        if _is_int(self.__table_size_bytes):  # pragma no branch (Defensive code)
            return long(self.__table_size_bytes)

    @property
    def table_status(self):
        """
        :type: None or string
        """
        if _is_str(self.__table_status):  # pragma no branch (Defensive code)
            return self.__table_status


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
        :type: None or string
        """
        if _is_str(self.__attribute_name):  # pragma no branch (Defensive code)
            return self.__attribute_name

    @property
    def attribute_type(self):
        """
        :type: None or string
        """
        if _is_str(self.__attribute_type):  # pragma no branch (Defensive code)
            return self.__attribute_type


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
        :type: None or string
        """
        if _is_str(self.__index_name):  # pragma no branch (Defensive code)
            return self.__index_name

    @property
    def index_size_bytes(self):
        """
        :type: None or long
        """
        if _is_int(self.__index_size_bytes):  # pragma no branch (Defensive code)
            return long(self.__index_size_bytes)

    @property
    def index_status(self):
        """
        :type: None or string
        """
        if _is_str(self.__index_status):  # pragma no branch (Defensive code)
            return self.__index_status

    @property
    def item_count(self):
        """
        :type: None or long
        """
        if _is_int(self.__item_count):  # pragma no branch (Defensive code)
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: None or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):  # pragma no branch (Defensive code)
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def projection(self):
        """
        :type: None or :class:`.Projection`
        """
        if _is_dict(self.__projection):  # pragma no branch (Defensive code)
            return Projection(**self.__projection)

    @property
    def provisioned_throughput(self):
        """
        :type: None or :class:`.ProvisionedThroughputDescription`
        """
        if _is_dict(self.__provisioned_throughput):  # pragma no branch (Defensive code)
            return ProvisionedThroughputDescription(**self.__provisioned_throughput)


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
        :type: None or list of string
        """
        if _is_list_of_str(self.__non_key_attributes):  # pragma no branch (Defensive code)
            return self.__non_key_attributes

    @property
    def projection_type(self):
        """
        :type: None or string
        """
        if _is_str(self.__projection_type):  # pragma no branch (Defensive code)
            return self.__projection_type


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
        :type: None or :class:`~datetime.datetime`
        """
        if _is_float(self.__last_decrease_date_time):  # pragma no branch (Defensive code)
            return datetime.datetime.utcfromtimestamp(self.__last_decrease_date_time)

    @property
    def last_increase_date_time(self):
        """
        :type: None or :class:`~datetime.datetime`
        """
        if _is_float(self.__last_increase_date_time):  # pragma no branch (Defensive code)
            return datetime.datetime.utcfromtimestamp(self.__last_increase_date_time)

    @property
    def number_of_decreases_today(self):
        """
        :type: None or long
        """
        if _is_int(self.__number_of_decreases_today):  # pragma no branch (Defensive code)
            return long(self.__number_of_decreases_today)

    @property
    def read_capacity_units(self):
        """
        :type: None or long
        """
        if _is_int(self.__read_capacity_units):  # pragma no branch (Defensive code)
            return long(self.__read_capacity_units)

    @property
    def write_capacity_units(self):
        """
        :type: None or long
        """
        if _is_int(self.__write_capacity_units):  # pragma no branch (Defensive code)
            return long(self.__write_capacity_units)


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
        :type: None or string
        """
        if _is_str(self.__attribute_name):  # pragma no branch (Defensive code)
            return self.__attribute_name

    @property
    def key_type(self):
        """
        :type: None or string
        """
        if _is_str(self.__key_type):  # pragma no branch (Defensive code)
            return self.__key_type


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
        :type: None or string
        """
        if _is_str(self.__index_name):  # pragma no branch (Defensive code)
            return self.__index_name

    @property
    def index_size_bytes(self):
        """
        :type: None or 
        """
        if _is_int(self.__index_size_bytes):  # pragma no branch (Defensive code)
            return long(self.__index_size_bytes)

    @property
    def item_count(self):
        """
        :type: None or long
        """
        if _is_int(self.__item_count):  # pragma no branch (Defensive code)
            return long(self.__item_count)

    @property
    def key_schema(self):
        """
        :type: None or list of :class:`.KeySchemaElement`
        """
        if _is_list_of_dict(self.__key_schema):  # pragma no branch (Defensive code)
            return [KeySchemaElement(**e) for e in self.__key_schema]

    @property
    def projection(self):
        """
        :type: None or 
        """
        if _is_dict(self.__projection):  # pragma no branch (Defensive code)
            return Projection(**self.__projection)


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

        :type: None or float
        """
        if _is_float(self.__capacity_units):  # pragma no branch (Defensive code)
            return float(self.__capacity_units)

    @property
    def global_secondary_indexes(self):
        """
        The capacity consumed on GSIs.

        :type: None or dict of string (index name) to :class:`.Capacity`
        """
        if _is_dict(self.__global_secondary_indexes):  # pragma no branch (Defensive code)
            return {n: Capacity(**v) for n, v in self.__global_secondary_indexes.iteritems()}

    @property
    def local_secondary_indexes(self):
        """
        The capacity consumed on LSIs.

        :type: None or dict of string (index name) to :class:`.Capacity`
        """
        if _is_dict(self.__local_secondary_indexes):  # pragma no branch (Defensive code)
            return {n: Capacity(**v) for n, v in self.__local_secondary_indexes.iteritems()}

    @property
    def table(self):
        """
        The capacity consumed on the table itself.

        :type: None or :class:`.Capacity`
        """
        if _is_dict(self.__table):  # pragma no branch (Defensive code)
            return Capacity(**self.__table)

    @property
    def table_name(self):
        """
        The name of the table.

        :type: None or string
        """
        if _is_str(self.__table_name):  # pragma no branch (Defensive code)
            return self.__table_name


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

        :type: None or float
        """
        if _is_float(self.__capacity_units):  # pragma no branch (Defensive code)
            return float(self.__capacity_units)


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

        :type: None or dict
        """
        if _is_dict(self.__item_collection_key):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__item_collection_key)

    @property
    def size_estimate_range_gb(self):
        """
        Range of sizes of the collection in GB.

        :type: None or list of two float
        """
        if _is_list_of_float(self.__size_estimate_range_gb):  # pragma no branch (Defensive code)
            return [float(e) for e in self.__size_estimate_range_gb]
