# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

class Operation(object):
    def __init__(self, operation):
        self.__operation = operation

    @property
    def name(self):
        return self.__operation


class OperationProxy(object):
    def __init__(self, operation):
        self._operation = operation

    def __getattr__(self, name):
        return getattr(self._operation, name)
