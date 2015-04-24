# coding: utf-8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import docutils.utils
import textwrap

import sphinx.environment


master_doc = "index"
project = "LowVoltage"
copyright = "2015, Vincent Jacques"
author = "Vincent Jacques"
extensions = []


nitpicky = True
nitpick_ignore = [
    ("py:class", "datetime.datetime")
]

# http://stackoverflow.com/a/28778969/905845
acknowledged_warnings = [
    "nonlocal image URI found: https://img.shields.io/travis/jacquev6/LowVoltage/master.svg",
    "nonlocal image URI found: https://img.shields.io/coveralls/jacquev6/LowVoltage/master.svg",
    "nonlocal image URI found: https://img.shields.io/codeclimate/github/jacquev6/LowVoltage.svg",
    "nonlocal image URI found: https://img.shields.io/pypi/dm/LowVoltage.svg",
    "nonlocal image URI found: https://img.shields.io/pypi/l/LowVoltage.svg",
    "nonlocal image URI found: https://img.shields.io/pypi/v/LowVoltage.svg",
    "nonlocal image URI found: https://pypip.in/py_versions/LowVoltage/badge.svg",
    "nonlocal image URI found: https://pypip.in/status/LowVoltage/badge.svg",
    "nonlocal image URI found: https://img.shields.io/github/issues/jacquev6/LowVoltage.svg",
    "nonlocal image URI found: https://img.shields.io/github/forks/jacquev6/LowVoltage.svg",
    "nonlocal image URI found: https://img.shields.io/github/stars/jacquev6/LowVoltage.svg",
]
def warn_node(self, msg, node):
    if msg not in acknowledged_warnings:
        self._warnfunc(msg, "%s:%s" % docutils.utils.get_source_line(node))
sphinx.environment.BuildEnvironment.warn_node = warn_node


# https://github.com/bitprophet/alabaster
# html_theme_path
extensions.append("alabaster")
html_theme = "alabaster"
html_sidebars = {
    "**": [
        "about.html", "navigation.html", "searchbox.html",
    ]
}
html_theme_options = {
    "github_user": "jacquev6",
    "github_repo": "LowVoltage",
    "github_banner": True,
    "travis_button": True,
}

# http://sphinx-doc.org/ext/autodoc.html
extensions.append("sphinx.ext.autodoc")
# autoclass_content
autodoc_member_order = "bysource"
autodoc_default_flags = ["members"]
# autodoc_docstring_signature
# autodoc_mock_imports
add_module_names = False
add_class_names = False


# http://sphinx-doc.org/ext/doctest.html
extensions.append("sphinx.ext.doctest")
# doctest_path
doctest_global_setup = textwrap.dedent("""
    from LowVoltage import *
    connection = make_connection("eu-west-1", EnvironmentCredentials())

    table = "LowVoltage.DocTests"
    table2 = "LowVoltage.DocTests2"

    try:
        connection(DescribeTable(table))
    except ResourceNotFoundException:
        connection(
            CreateTable(table)
                .hash_key("h", NUMBER).provisioned_throughput(1, 1)
                .global_secondary_index("gsi").hash_key("gh", NUMBER).range_key("gr", NUMBER).provisioned_throughput(1, 1).project_all()
        )

    try:
        connection(DescribeTable(table2))
    except ResourceNotFoundException:
        connection(
            CreateTable(table2)
                .hash_key("h", NUMBER).range_key("r1", NUMBER).provisioned_throughput(1, 1)
                .local_secondary_index("lsi").hash_key("h", NUMBER).range_key("r2", NUMBER).project_all()
        )

    WaitForTableActivation(connection, table)
    BatchPutItem(
        connection,
        table,
        [{"h": h, "gh": 0, "gr": 0} for h in range(10)],
    )

    WaitForTableActivation(connection, table2)
    BatchPutItem(
        connection,
        table2,
        [{"h": h, "r1": 0, "r2": 0} for h in range(10)],
    )
    """)
# doctest_global_cleanup
# doctest_test_doctest_blocks
