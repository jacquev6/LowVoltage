# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

from LowVoltage.connection import (
    BasicConnectionUnitTests, BasicConnectionIntegTests,
    RetryingConnectionUnitTests, RetryingConnectionIntegTests
)
from LowVoltage.policies import (
    FailFastErrorPolicyUnitTests,
    ExponentialBackoffErrorPolicyUnitTests,
)
from LowVoltage.operations.expressions import ConditionExpressionUnitTests
from LowVoltage.operations.conversion import ConversionUnitTests

from LowVoltage.operations.return_mixins import (
    ReturnValuesMixinUnitTests,
    ReturnOldValuesMixinUnitTests,
    ReturnConsumedCapacityMixinUnitTests,
    ReturnItemCollectionMetricsMixinUnitTests,
)
from LowVoltage.operations.expression_mixins import (
    ExpressionAttributeNamesMixinUnitTests,
    ExpressionAttributeValuesMixinUnitTests,
    ConditionExpressionMixinUnitTests,
    ProjectionExpressionMixinUnitTests,
    FilterExpressionMixinUnitTests,
)

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
    ScanUnitTests, ScanIntegTests,
    QueryUnitTests, QueryIntegTests,
)

from LowVoltage.tests.cover import *
