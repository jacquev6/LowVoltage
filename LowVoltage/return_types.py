# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>


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
        # self.creation_date_time = CreationDateTime  # @todo datetime
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
        self.last_decrease_date_time = LastDecreaseDateTime  # @todo datetime
        self.last_increase_date_time = LastIncreaseDateTime  # @todo datetime
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
