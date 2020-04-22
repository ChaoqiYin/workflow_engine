# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
VARIABLES_MAP = {int: 'long_', str: 'text_', float: 'double_', bool: 'long_'}
VARIABLES_TYPE_MAP = {'long_': 'integer', 'text_': 'string', 'double_': 'double'}
REVERSE_VARIABLES_TYPE_MAP = dict(zip(VARIABLES_TYPE_MAP.values(), VARIABLES_TYPE_MAP.keys()))


def join_tag_name(start_tag_name, tag_name):
    '''
    因为工具解析的节点为bpmn而js解析的节点为bpmn2，故需要拼接
    :param tag_name:
    :return:
    '''
    return '{}:{}'.format(start_tag_name, tag_name)


def get_variable_values(variables):
    '''
    组装存入数据库需要的变量数据列表
    :param variables:
    :return:
    '''
    variable_values = []
    bool_map = {True: 1, False: 0}
    for key, value in variables.items():
        assert type(value) in VARIABLES_MAP or value is None, ValueError('variable must be int/float/str/bool/None')
        this_dict = {
            'type': VARIABLES_TYPE_MAP[VARIABLES_MAP[type(value)]] if value is not None else 'null',
            'name': key
        }
        if value is not None:
            if isinstance(value, bool):
                this_dict[VARIABLES_MAP[type(value)]] = bool_map[value]
            else:
                this_dict[VARIABLES_MAP[type(value)]] = value
        variable_values.append(this_dict)
    return variable_values


def get_variable_real_dict(records):
    '''
    将数据库的变量记录解析为dict
    :param records:
    :return:
    '''
    real_dict = {}
    for record in records:
        variable_type = record['type']
        real_dict[record['name']] = record[REVERSE_VARIABLES_TYPE_MAP[variable_type]]
    return real_dict
