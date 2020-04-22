# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
import re
import uuid
from .base import ParseBase
from ..utils import join_tag_name, VARIABLES_MAP, VARIABLES_TYPE_MAP


class ParseTask(ParseBase):
    '''
    普通任务
    '''

    def parse(self, element):
        self.tag_name = element.localName
        self.name = element.getAttribute('name').replace('\'', '\"') or None
        self.tag_type = element.nodeType
        self.tag_id = element.getAttribute('id')
        self.properties = {}
        self.incoming = {
            'tag_id': child.firstChild.data for child in
            element.getElementsByTagName(join_tag_name(self.start_tag_name, 'incoming'))
        }
        self.outgoing = {
            'tag_id': child.firstChild.data for child in
            element.getElementsByTagName(join_tag_name(self.start_tag_name, 'outgoing'))
        }
        return self


class ParseUserTask(ParseTask):
    '''
    用户任务
    '''

    def set_role(self, element):
        attributes = element.attributes._attrs
        for attr_key in attributes:
            if attributes[attr_key].localName == 'assignee':
                self.properties['assignee'] = attributes[attr_key].value
            elif attributes[attr_key].localName == 'candidateGroups':
                self.properties['candidateGroups'] = attributes[attr_key].value
            elif attributes[attr_key].localName == 'candidateUsers':
                self.properties['candidateUsers'] = attributes[attr_key].value

    def parse(self, element):
        super().parse(element)
        self.set_role(element)
        return self

    @staticmethod
    def get_variables_or_string(info, re_compile):
        result = {'string': [], 'variables': []}
        this_list = info.split(',')
        for i in this_list:
            if re.match(re_compile, i) is None:
                result['string'].append(i)
            else:
                result['variables'].append(i)
        return result

    def __set_result_value(self, key, key2, result, role_info, re_compile, execution_id):
        if len(result['string']) != 0:
            role_info[key] = result['string']
        if len(result['variables']) != 0:
            this_list = []
            for var in result['variables']:
                variable_name = re.match(re_compile, var).groups()[0]
                variable_record = self.sql_class(db_name='act_ru_variable', cr=self.cr).select(
                    ['id'], "execution_id='{}' and name='{}'".format(execution_id, variable_name)
                )
                assert len(variable_record) == 1, "the variable's length is not 1 where name is $({})".format(variable_name)
                this_list.append(variable_record[0]['id'])
            role_info[key2] = this_list

    def get_role_info(self, tag_id, act_run_execution_record):
        properties = self.elements_info[tag_id]['properties']
        # user_id是变量形式的用户id，group_id同理
        role_info = {'users': None, 'user_id': None, 'groups': None, 'group_id': None}
        re_compile = re.compile(r'\$\((.*)\)')
        execution_id = act_run_execution_record[0]['execution_id']
        for key in properties:
            if key == 'candidateUsers':
                result = self.get_variables_or_string(properties[key], re_compile)
                self.__set_result_value('users', 'user_id', result, role_info, re_compile, execution_id)
            elif key == 'candidateGroups':
                result = self.get_variables_or_string(properties[key], re_compile)
                self.__set_result_value('groups', 'group_id', result, role_info, re_compile, execution_id)
            # TODO: elif key == 'assignee'
        return role_info

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        # 添加用户组
        act_run_execution_record = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            [], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) == 1, ValueError(
            'there is no run_execution where proc_inst_id is "{}"'.format(proc_inst_id))
        role_info = self.get_role_info(tag_id, act_run_execution_record)
        # 开始创建task任务
        task_id = uuid.uuid4()
        self.sql_class(db_name='act_run_task', cr=self.cr).insert({
            'parent_task_id': parent_task_id,
            'task_id': task_id, 'task_key': tag_id, 'execution_id': act_run_execution_record[0]['execution_id'],
            'proc_inst_id': proc_inst_id, 'proc_def_id': act_run_execution_record[0]['proc_def_id'],
            'name': self.elements_info[tag_id]['name'], 'priority': 50,
        })
        self.sql_class(db_name='act_run_execution', cr=self.cr).update(
            {'act_id': tag_id}, "proc_inst_id='{}'".format(proc_inst_id)
        )
        # 绑定相应用户或组
        m2m_sql_class = self.sql_class(db_name='act_run_task_m2m_relation', cr=self.cr)
        for info in role_info:  # role_info => {'users': None, 'user_id': None, 'groups': None, 'group_id': None}
            if role_info[info] is not None:
                type_value = 'user' if info in ['users', 'user_id'] else 'group'
                for value in role_info[info]:  # value => ['value1', 'value1']
                    insert_value = dict(
                        type=type_value, task_id=task_id
                    )
                    # 是string值的话
                    if info in ['users', 'groups']:
                        insert_value['execution_id'] = act_run_execution_record[0]['execution_id']
                        insert_value['is_variable'] = 0
                        insert_value['variable_type'] = VARIABLES_MAP[type(value)]
                        insert_value[VARIABLES_MAP[type(value)]] = value
                    # 是变量值
                    else:
                        insert_value['execution_id'] = act_run_execution_record[0]['execution_id']
                        insert_value['is_variable'] = 1
                        insert_value['variable_id'] = value
                    m2m_sql_class.insert(insert_value)

