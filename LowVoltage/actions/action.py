# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>


class Action(object):
    is_completable = False
    is_waitable = False

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
