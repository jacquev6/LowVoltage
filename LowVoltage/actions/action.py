# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

class Action(object):
    def __init__(self, action):
        self.__action = action

    @property
    def name(self):
        return self.__action


class ActionProxy(object):
    def __init__(self, action):
        self._action = action

    def __getattr__(self, name):
        return getattr(self._action, name)
