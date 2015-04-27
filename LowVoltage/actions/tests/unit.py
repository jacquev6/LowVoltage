# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

from ..conversion import ConversionUnitTests
from ..expressions import ConditionExpressionUnitTests
from ..return_types import (
    TableDescriptionUnitTests,
    AttributeDefinitionUnitTests,
    GlobalSecondaryIndexDescriptionUnitTests,
    ProjectionUnitTests,
    ProvisionedThroughputDescriptionUnitTests,
    KeySchemaElementUnitTests,
    LocalSecondaryIndexDescriptionUnitTests,
    ConsumedCapacityUnitTests,
    CapacityUnitTests,
    ItemCollectionMetricsUnitTests,
)

from ..batch_get_item import BatchGetItemUnitTests, BatchGetItemResponseUnitTests
from ..batch_write_item import BatchWriteItemUnitTests, BatchWriteItemResponseUnitTests
from ..create_table import CreateTableUnitTests, CreateTableResponseUnitTests
from ..delete_item import DeleteItemUnitTests, DeleteItemResponseUnitTests
from ..delete_table import DeleteTableUnitTests, DeleteTableResponseUnitTests
from ..describe_table import DescribeTableUnitTests, DescribeTableResponseUnitTests
from ..get_item import GetItemUnitTests, GetItemResponseUnitTests
from ..list_tables import ListTablesUnitTests, ListTablesResponseUnitTests
from ..put_item import PutItemUnitTests, PutItemResponseUnitTests
from ..query import QueryUnitTests, QueryResponseUnitTests
from ..scan import ScanUnitTests, ScanResponseUnitTests
from ..update_item import UpdateItemUnitTests, UpdateItemResponseUnitTests
from ..update_table import UpdateTableUnitTests, UpdateTableResponseUnitTests
