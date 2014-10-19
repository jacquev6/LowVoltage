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
        # self.attribute_definitions = [AttributeDefinition(**d) for d in AttributeDefinitions]
        self.creation_date_time = CreationDateTime  # @todo datetime.datetime
        # self.global_secondary_indexes = [GlobalSecondaryIndexDescription(**d) for d in GlobalSecondaryIndexes]
        self.item_count = None if ItemCount is None else long(ItemCount)
        # self.key_schema = [KeySchemaElement(**e) for e in KeySchema]
        # self.local_secondary_indexes = [LocalSecondaryIndexDescription(**d) for d in LocalSecondaryIndexes]
        # self.provisioned_throughput = ProvisionedThroughputDescription(**ProvisionedThroughput)
        self.table_name = TableName
        self.table_size_bytes = None if TableSizeBytes is None else long(TableSizeBytes)
        self.table_status = TableStatus
