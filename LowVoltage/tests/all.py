# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

from LowVoltage.connection import ConnectionUnitTests, ConnectionIntegTests
from LowVoltage.operations.expressions import ConditionExpressionUnitTests

from LowVoltage.operations.admin_operations import (
    CreateTableUnitTests, CreateTableIntegTests, CreateTableErrorTests,
    DeleteTableUnitTests, DeleteTableIntegTests,
    DescribeTableUnitTests, DescribeTableIntegTests,
    ListTablesUnitTests, ListTablesIntegTests,
    UpdateTableUnitTests, UpdateTableIntegTests,
)
from LowVoltage.operations.item_operations import (
    DeleteItemUnitTests,
    GetItemUnitTests,
    PutItemUnitTests,
    UpdateItemUnitTests,
)
from LowVoltage.operations.batch_operations import (
    BatchGetItemUnitTests,
    BatchWriteItemUnitTests,
)

from LowVoltage.tests.exploration import *
