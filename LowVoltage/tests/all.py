# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

from LowVoltage.connection import ConnectionUnitTests, ConnectionIntegTests
from LowVoltage.operations.expressions import ConditionExpressionUnitTests
from LowVoltage.operations.conversion import ConversionUnitTests

from LowVoltage.operations.admin_operations import (
    CreateTableUnitTests, CreateTableIntegTests, CreateTableErrorTests,
    DeleteTableUnitTests, DeleteTableIntegTests,
    DescribeTableUnitTests, DescribeTableIntegTests,
    ListTablesUnitTests, ListTablesIntegTests,
    UpdateTableUnitTests, UpdateTableIntegTests,
)
from LowVoltage.operations.item_operations import (
    DeleteItemUnitTests, DeleteItemIntegTests,
    GetItemUnitTests, GetItemIntegTests,
    PutItemUnitTests, PutItemIntegTests,
    UpdateItemUnitTests, UpdateItemIntegTests,
)
from LowVoltage.operations.batch_operations import (
    BatchGetItemUnitTests, BatchGetItemIntegTests,
    BatchWriteItemUnitTests, BatchWriteItemIntegTests,
)

from LowVoltage.tests.exploration import *

from LowVoltage.tests.cover import *
