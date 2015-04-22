# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>


class Iterator(object):
    # Don't implement anything else than a single forward iteration.
    # Remember PyGithub's PaginatedList; it was too difficult to maintain for niche use-cases.
    # Clients can use raw actions to implement their specific needs.

    def __init__(self, connection, first_action):
        self.__connection = connection
        self.__current_iter = [].__iter__()
        self.__next_action = first_action

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.__current_iter.next()
        except StopIteration:
            if self.__next_action is None:
                raise
            else:
                r = self.__connection(self.__next_action)
                next_action, new_items = self.process(self.__next_action, r)
                self.__next_action = next_action
                self.__current_iter = new_items.__iter__()
                return self.__current_iter.next()
