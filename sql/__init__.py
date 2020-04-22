# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from .postgresql import Posetgresql


__registry_db = {
    'mysql': None,
    'postgresql': Posetgresql
}


def get_sql_class(db_type):
    return __registry_db[db_type]
