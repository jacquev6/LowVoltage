# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

from .batch_delete_item import BatchDeleteItem
from .batch_get_item_iterator import BatchGetItemIterator
from .batch_put_item import BatchPutItem
from .list_tables_iterator import ListTablesIterator
from .query_iterator import QueryIterator
from .scan_iterator import ScanIterator

# @todo Review the interfaces in this package: some accept a reference Action (Query, Scan), others don't
# Also it's weird that Actions are passed to the connection, but compounds accept the conneciton as an argument
