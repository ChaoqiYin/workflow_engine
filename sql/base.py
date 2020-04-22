# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias


class BaseSql(object):

    @classmethod
    def db_exist(cls, cr):
        raise NotImplementedError("You must implement the db_exist method.")