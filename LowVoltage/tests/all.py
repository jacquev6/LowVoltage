# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

from LowVoltage.connection import (
    BasicConnectionUnitTests, BasicConnectionIntegTests,
    RetryingConnectionUnitTests, RetryingConnectionIntegTests
)
from LowVoltage.policies import (
    FailFastErrorPolicyUnitTests,
    ExponentialBackoffErrorPolicyUnitTests,
)
from LowVoltage.actions.expressions import ConditionExpressionUnitTests
from LowVoltage.actions.conversion import ConversionUnitTests

from LowVoltage.actions.return_mixins import (
    ReturnValuesMixinUnitTests,
    ReturnOldValuesMixinUnitTests,
    ReturnConsumedCapacityMixinUnitTests,
    ReturnItemCollectionMetricsMixinUnitTests,
)
from LowVoltage.actions.expression_mixins import (
    ExpressionAttributeNamesMixinUnitTests,
    ExpressionAttributeValuesMixinUnitTests,
    ConditionExpressionMixinUnitTests,
    ProjectionExpressionMixinUnitTests,
    FilterExpressionMixinUnitTests,
)

from LowVoltage.actions.admin_actions import (
    CreateTableUnitTests, CreateTableIntegTests, CreateTableErrorTests,
    DeleteTableUnitTests, DeleteTableIntegTests,
    DescribeTableUnitTests, DescribeTableIntegTests,
    ListTablesUnitTests, ListTablesIntegTests,
    UpdateTableUnitTests, UpdateTableIntegTests,
)
from LowVoltage.actions.item_actions import (
    DeleteItemUnitTests, DeleteItemIntegTests,
    GetItemUnitTests, GetItemIntegTests,
    PutItemUnitTests, PutItemIntegTests,
    UpdateItemUnitTests, UpdateItemIntegTests,
)
from LowVoltage.actions.batch_actions import (
    BatchGetItemUnitTests, BatchGetItemIntegTests,
    BatchWriteItemUnitTests, BatchWriteItemIntegTests,
    ScanUnitTests, ScanIntegTests,
    QueryUnitTests, QueryIntegTests,
)

from LowVoltage.tests.cover import *
