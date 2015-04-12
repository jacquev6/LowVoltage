# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

from LowVoltage.actions.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value


class TableDescription:
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
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html
        self.attribute_definitions = None if AttributeDefinitions is None else [AttributeDefinition(**d) for d in AttributeDefinitions]
        self.creation_date_time = None if CreationDateTime is None else datetime.datetime.utcfromtimestamp(CreationDateTime)
        self.global_secondary_indexes = None if GlobalSecondaryIndexes is None else [GlobalSecondaryIndexDescription(**d) for d in GlobalSecondaryIndexes]
        self.item_count = None if ItemCount is None else long(ItemCount)
        self.key_schema = None if KeySchema is None else [KeySchemaElement(**e) for e in KeySchema]
        self.local_secondary_indexes = None if LocalSecondaryIndexes is None else [LocalSecondaryIndexDescription(**d) for d in LocalSecondaryIndexes]
        self.provisioned_throughput = None if ProvisionedThroughput is None else ProvisionedThroughputDescription(**ProvisionedThroughput)
        self.table_name = TableName
        self.table_size_bytes = None if TableSizeBytes is None else long(TableSizeBytes)
        self.table_status = TableStatus


class AttributeDefinition:
    def __init__(
        self,
        AttributeName=None,
        AttributeType=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeDefinition.html
        self.attribute_name = AttributeName
        self.attribute_type = AttributeType


class GlobalSecondaryIndexDescription:
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
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GlobalSecondaryIndexDescription.html
        self.index_name = IndexName
        self.index_size_bytes = IndexSizeBytes
        self.index_status = IndexStatus
        self.item_count = None if ItemCount is None else long(ItemCount)
        self.key_schema = None if KeySchema is None else [KeySchemaElement(**e) for e in KeySchema]
        self.projection = None if Projection is None else globals()["Projection"](**Projection)  # Ahem...
        self.provisioned_throughput = None if ProvisionedThroughput is None else ProvisionedThroughputDescription(**ProvisionedThroughput)


class Projection:
    def __init__(
        self,
        NonKeyAttributes=None,
        ProjectionType=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Projection.html
        self.non_key_attributes = NonKeyAttributes
        self.projection_type = ProjectionType


class ProvisionedThroughputDescription:
    def __init__(
        self,
        LastDecreaseDateTime=None,
        LastIncreaseDateTime=None,
        NumberOfDecreasesToday=None,
        ReadCapacityUnits=None,
        WriteCapacityUnits=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ProvisionedThroughputDescription.html
        self.last_decrease_date_time = None if LastDecreaseDateTime is None else datetime.datetime.utcfromtimestamp(LastDecreaseDateTime)
        self.last_increase_date_time = None if LastIncreaseDateTime is None else datetime.datetime.utcfromtimestamp(LastIncreaseDateTime)
        self.number_of_decreases_today = NumberOfDecreasesToday
        self.read_capacity_units = ReadCapacityUnits
        self.write_capacity_units = WriteCapacityUnits


class KeySchemaElement:
    def __init__(
        self,
        AttributeName=None,
        KeyType=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeySchemaElement.html
        self.attribute_name = AttributeName
        self.key_type = KeyType


class LocalSecondaryIndexDescription:
    def __init__(
        self,
        IndexName=None,
        IndexSizeBytes=None,
        IndexStatus=None,
        ItemCount=None,
        KeySchema=None,
        Projection=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_LocalSecondaryIndexDescription.html
        self.index_name = IndexName
        self.index_size_bytes = IndexSizeBytes
        self.index_status = IndexStatus
        self.item_count = None if ItemCount is None else long(ItemCount)
        self.key_schema = None if KeySchema is None else [KeySchemaElement(**e) for e in KeySchema]
        self.projection = None if Projection is None else globals()["Projection"](**Projection)


class ConsumedCapacity:
    def __init__(
        self,
        CapacityUnits=None,
        GlobalSecondaryIndexes=None,
        LocalSecondaryIndexes=None,
        Table=None,
        TableName=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ConsumedCapacity.html
        self.capacity_units = None if CapacityUnits is None else float(CapacityUnits)
        self.global_secondary_indexes = None if GlobalSecondaryIndexes is None else {n: Capacity(**v) for n, v in GlobalSecondaryIndexes.iteritems()}
        self.local_secondary_indexes = None if LocalSecondaryIndexes is None else {n: Capacity(**v) for n, v in LocalSecondaryIndexes.iteritems()}
        self.table = None if Table is None else Capacity(**Table)
        self.table_name = TableName


class Capacity:
    def __init__(
        self,
        CapacityUnits=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Capacity.html
        self.capacity_units = None if CapacityUnits is None else float(CapacityUnits)


class ItemCollectionMetrics:
    def __init__(
        self,
        ItemCollectionKey=None,
        SizeEstimateRangeGB=None,
        **dummy
    ):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ItemCollectionMetrics.html
        self.item_collection_key = None if ItemCollectionKey is None else _convert_db_to_dict(ItemCollectionKey)
        self.size_estimate_range_gb = None if SizeEstimateRangeGB is None else [float(e) for e in SizeEstimateRangeGB]
