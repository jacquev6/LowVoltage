# coding: utf-8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import docutils.utils

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
doctest_global_setup = "import LowVoltage.testing.doc_tests; from LowVoltage import *; (connection, table, table2) = LowVoltage.testing.doc_tests.global_setup()"
# doctest_global_cleanup
doctest_test_doctest_blocks=True
