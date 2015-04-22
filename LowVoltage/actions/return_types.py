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


class TableDescription_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html"""

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
        self.attribute_definitions = None
        if _is_list_of_dict(AttributeDefinitions):  # pragma no branch (Defensive code)
            self.attribute_definitions = [AttributeDefinition_(**d) for d in AttributeDefinitions]

        self.creation_date_time = None
        if _is_float(CreationDateTime):  # pragma no branch (Defensive code)
            self.creation_date_time = datetime.datetime.utcfromtimestamp(CreationDateTime)

        self.global_secondary_indexes = None
        if _is_list_of_dict(GlobalSecondaryIndexes):  # pragma no branch (Defensive code)
            self.global_secondary_indexes = [GlobalSecondaryIndexDescription_(**d) for d in GlobalSecondaryIndexes]

        self.item_count = None
        if _is_int(ItemCount):  # pragma no branch (Defensive code)
            self.item_count = long(ItemCount)

        self.key_schema = None
        if _is_list_of_dict(KeySchema):  # pragma no branch (Defensive code)
            self.key_schema = [KeySchemaElement_(**e) for e in KeySchema]

        self.local_secondary_indexes = None
        if _is_list_of_dict(LocalSecondaryIndexes):  # pragma no branch (Defensive code)
            self.local_secondary_indexes = [LocalSecondaryIndexDescription_(**d) for d in LocalSecondaryIndexes]

        self.provisioned_throughput = None
        if _is_dict(ProvisionedThroughput):  # pragma no branch (Defensive code)
            self.provisioned_throughput = ProvisionedThroughputDescription_(**ProvisionedThroughput)

        self.table_name = None
        if _is_str(TableName):  # pragma no branch (Defensive code)
            self.table_name = TableName

        self.table_size_bytes = None
        if _is_int(TableSizeBytes):  # pragma no branch (Defensive code)
            self.table_size_bytes = long(TableSizeBytes)

        self.table_status = None
        if _is_str(TableStatus):  # pragma no branch (Defensive code)
            self.table_status = TableStatus


class AttributeDefinition_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeDefinition.html"""

    def __init__(
        self,
        AttributeName=None,
        AttributeType=None,
        **dummy
    ):
        self.attribute_name = None
        if _is_str(AttributeName):  # pragma no branch (Defensive code)
            self.attribute_name = AttributeName

        self.attribute_type = None
        if _is_str(AttributeType):  # pragma no branch (Defensive code)
            self.attribute_type = AttributeType


class GlobalSecondaryIndexDescription_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GlobalSecondaryIndexDescription.html"""

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
        self.index_name = None
        if _is_str(IndexName):  # pragma no branch (Defensive code)
            self.index_name = IndexName

        self.index_size_bytes = None
        if _is_int(IndexSizeBytes):  # pragma no branch (Defensive code)
            self.index_size_bytes = long(IndexSizeBytes)

        self.index_status = None
        if _is_str(IndexStatus):  # pragma no branch (Defensive code)
            self.index_status = IndexStatus

        self.item_count = None
        if _is_int(ItemCount):  # pragma no branch (Defensive code)
            self.item_count = long(ItemCount)

        self.key_schema = None
        if _is_list_of_dict(KeySchema):  # pragma no branch (Defensive code)
            self.key_schema = [KeySchemaElement_(**e) for e in KeySchema]

        self.projection = None
        if _is_dict(Projection):  # pragma no branch (Defensive code)
            self.projection = Projection_(**Projection)

        self.provisioned_throughput = None
        if _is_dict(ProvisionedThroughput):  # pragma no branch (Defensive code)
            self.provisioned_throughput = ProvisionedThroughputDescription_(**ProvisionedThroughput)


class Projection_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Projection.html"""

    def __init__(
        self,
        NonKeyAttributes=None,
        ProjectionType=None,
        **dummy
    ):
        self.non_key_attributes = None
        if _is_list_of_str(NonKeyAttributes):  # pragma no branch (Defensive code)
            self.non_key_attributes = NonKeyAttributes

        self.projection_type = None
        if _is_str(ProjectionType):  # pragma no branch (Defensive code)
            self.projection_type = ProjectionType


class ProvisionedThroughputDescription_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ProvisionedThroughputDescription.html"""

    def __init__(
        self,
        LastDecreaseDateTime=None,
        LastIncreaseDateTime=None,
        NumberOfDecreasesToday=None,
        ReadCapacityUnits=None,
        WriteCapacityUnits=None,
        **dummy
    ):
        self.last_decrease_date_time = None
        if _is_float(LastDecreaseDateTime):  # pragma no branch (Defensive code)
            self.last_decrease_date_time = datetime.datetime.utcfromtimestamp(LastDecreaseDateTime)

        self.last_increase_date_time = None
        if _is_float(LastIncreaseDateTime):  # pragma no branch (Defensive code)
            self.last_increase_date_time = datetime.datetime.utcfromtimestamp(LastIncreaseDateTime)

        self.number_of_decreases_today = None
        if _is_int(NumberOfDecreasesToday):  # pragma no branch (Defensive code)
            self.number_of_decreases_today = long(NumberOfDecreasesToday)

        self.read_capacity_units = None
        if _is_int(ReadCapacityUnits):  # pragma no branch (Defensive code)
            self.read_capacity_units = long(ReadCapacityUnits)

        self.write_capacity_units = None
        if _is_int(WriteCapacityUnits):  # pragma no branch (Defensive code)
            self.write_capacity_units = long(WriteCapacityUnits)


class KeySchemaElement_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeySchemaElement.html"""

    def __init__(
        self,
        AttributeName=None,
        KeyType=None,
        **dummy
    ):
        self.attribute_name = None
        if _is_str(AttributeName):  # pragma no branch (Defensive code)
            self.attribute_name = AttributeName

        self.key_type = None
        if _is_str(KeyType):  # pragma no branch (Defensive code)
            self.key_type = KeyType


class LocalSecondaryIndexDescription_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_LocalSecondaryIndexDescription.html"""

    def __init__(
        self,
        IndexName=None,
        IndexSizeBytes=None,
        ItemCount=None,
        KeySchema=None,
        Projection=None,
        **dummy
    ):
        self.index_name = None
        if _is_str(IndexName):  # pragma no branch (Defensive code)
            self.index_name = IndexName

        self.index_size_bytes = None
        if _is_int(IndexSizeBytes):  # pragma no branch (Defensive code)
            self.index_size_bytes = long(IndexSizeBytes)

        self.item_count = None
        if ItemCount is not None:  # pragma no branch (Defensive code)
            self.item_count = long(ItemCount)

        self.key_schema = None
        if KeySchema is not None:  # pragma no branch (Defensive code)
            self.key_schema = [KeySchemaElement_(**e) for e in KeySchema]

        self.projection = None
        if Projection is not None:  # pragma no branch (Defensive code)
            self.projection = Projection_(**Projection)


class ConsumedCapacity_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ConsumedCapacity.html"""

    def __init__(
        self,
        CapacityUnits=None,
        GlobalSecondaryIndexes=None,
        LocalSecondaryIndexes=None,
        Table=None,
        TableName=None,
        **dummy
    ):
        self.capacity_units = None
        if _is_float(CapacityUnits):  # pragma no branch (Defensive code)
            self.capacity_units = float(CapacityUnits)

        self.global_secondary_indexes = None
        if _is_dict(GlobalSecondaryIndexes):  # pragma no branch (Defensive code)
            self.global_secondary_indexes = {n: Capacity_(**v) for n, v in GlobalSecondaryIndexes.iteritems()}

        self.local_secondary_indexes = None
        if _is_dict(LocalSecondaryIndexes):  # pragma no branch (Defensive code)
            self.local_secondary_indexes = {n: Capacity_(**v) for n, v in LocalSecondaryIndexes.iteritems()}

        self.table = None
        if _is_dict(Table):  # pragma no branch (Defensive code)
            self.table = Capacity_(**Table)

        self.table_name = None
        if _is_str(TableName):  # pragma no branch (Defensive code)
            self.table_name = TableName


class Capacity_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Capacity.html"""

    def __init__(
        self,
        CapacityUnits=None,
        **dummy
    ):
        self.capacity_units = None
        if _is_float(CapacityUnits):  # pragma no branch (Defensive code)
            self.capacity_units = float(CapacityUnits)


class ItemCollectionMetrics_(object):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ItemCollectionMetrics.html"""

    def __init__(
        self,
        ItemCollectionKey=None,
        SizeEstimateRangeGB=None,
        **dummy
    ):
        self.item_collection_key = None
        if _is_dict(ItemCollectionKey):  # pragma no branch (Defensive code)
            self.item_collection_key = _convert_db_to_dict(ItemCollectionKey)

        self.size_estimate_range_gb = None
        if _is_list_of_float(SizeEstimateRangeGB):  # pragma no branch (Defensive code)
            self.size_estimate_range_gb = [float(e) for e in SizeEstimateRangeGB]
