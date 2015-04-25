# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv


def global_setup():
    connection = _lv.Connection("us-west-2", _lv.EnvironmentCredentials())

    table1 = "LowVoltage.Tests.Doc.1"
    table2 = "LowVoltage.Tests.Doc.2"

    try:
        connection(_lv.DescribeTable(table1))
    except _lv.ResourceNotFoundException:
        connection(
            _lv.CreateTable(table)
                .hash_key("h", _lv.NUMBER).provisioned_throughput(1, 1)
                .global_secondary_index("gsi").hash_key("gh", _lv.NUMBER).range_key("gr", _lv.NUMBER).provisioned_throughput(1, 1).project_all()
        )

    try:
        connection(_lv.DescribeTable(table2))
    except _lv.ResourceNotFoundException:
        connection(
            _lv.CreateTable(table2)
                .hash_key("h", _lv.NUMBER).range_key("r1", _lv.NUMBER).provisioned_throughput(1, 1)
                .local_secondary_index("lsi").hash_key("h", _lv.NUMBER).range_key("r2", _lv.NUMBER).project_all()
        )

    _lv.WaitForTableActivation(connection, table1)
    _lv.BatchPutItem(
        connection,
        table1,
        [{"h": h, "gh": 0, "gr": 0} for h in range(10)],
    )

    _lv.WaitForTableActivation(connection, table2)
    _lv.BatchPutItem(
        connection,
        table2,
        [{"h": h, "r1": 0, "r2": 0} for h in range(10)],
    )

    _lv.BatchPutItem(
        connection,
        table2,
        [{"h": 42, "r1": r1, "r2": 10 - r1} for r1 in range(6)],
    )

    _lv.BatchPutItem(
        connection,
        table2,
        [{"h": 42, "r1": r1} for r1 in range(6, 10)],
    )

    return connection, table1, table2
