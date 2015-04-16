# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>


class WaitingConnection(object):
    """Connection decorator waiting until admin actions are done (until table's state is ACTIVE)"""

    def __init__(self, connection):
        self.__connection = connection

    def request(self, action):
        # @todo Implement (should wait until CreateTable, UpdateTable and DeleteTable's effect is applied)
        return self.__connection.request(action)
