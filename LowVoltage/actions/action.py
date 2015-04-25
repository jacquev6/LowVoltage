# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>


class Action(object):
    def __init__(self, name, response_class):
        self.name = name
        self.response_class = response_class


class ActionProxy(object):
    def __init__(self, action):
        self._action = action

    def __getattr__(self, name):
        return getattr(self._action, name)
