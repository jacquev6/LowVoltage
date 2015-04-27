# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

from .test_batch_get_item import BatchGetItemLocalIntegTests
from .test_batch_write_item import BatchWriteItemLocalIntegTests
from .test_create_table import CreateTableLocalIntegTests, CreateTableErrorLocalIntegTests
from .test_delete_item import DeleteItemLocalIntegTests
from .test_delete_table import DeleteTableLocalIntegTests
from .test_describe_table import DescribeTableLocalIntegTests
from .test_get_item import GetItemLocalIntegTests
from .test_list_tables import ListTablesLocalIntegTests
from .test_put_item import PutItemLocalIntegTests
from .test_query import QueryLocalIntegTests
from .test_scan import ScanLocalIntegTests
from .test_update_item import UpdateItemLocalIntegTests
from .test_update_table import UpdateTableLocalIntegTests
