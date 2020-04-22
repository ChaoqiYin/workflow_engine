# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
import json
import re
import uuid
import os
import configparser

from ..sql import get_sql_class
from ..parse import get_element_class
from ..utils import get_variable_values, get_variable_real_dict

DIR = os.path.dirname(__file__)
CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(DIR), 'config.conf'))


class WorkFlowAction(object):
    def __init__(self, cr):
        self.db_type = CONFIG.get('options', 'db_type') if CONFIG.has_option('options', 'db_type') else 'postgresql'
        self.cr = cr
        self.sql_class = get_sql_class(self.db_type)

    def save_workflow(self, tree):
        '''
        save the flow info to database
        :param tree
        :return:
        '''
        self.sql_class.db_exist(self.cr)
        if len(tree.elements_info['elements_info']) != 0:
            tree_json = json.dumps(tree.elements_info)
            self.__write_work_flow(
                tree.elements_info['process_id'], tree.elements_info['name'], tree.elements_info['version'], tree_json
            )
        else:
            raise ValueError('the elements_info can\'t have no info')

    def __write_work_flow(self, process_id, name, version, tree_json):
        '''
        insert or update record
        :param process_id:
        :param name:
        :param version:
        :param tree_json:
        :return:
        '''
        act_re_procdef = self.sql_class(db_name='act_re_procdef', cr=self.cr)
        result = act_re_procdef.select_count("key='{}'".format(process_id))
        deployment_id = uuid.uuid4()  # 生成随机的deployment_id
        if result > 0:
            rowcount = act_re_procdef.update(
                {'name': name, 'deployment_id': deployment_id, 'model_editor_json': tree_json, 'version': version},
                "key='{}'".format(process_id)
            )
        else:
            rowcount = act_re_procdef.insert({
                'name': name, 'key': process_id, 'deployment_id': deployment_id, 'model_editor_json': tree_json,
                'version': version
            })
        return rowcount

    def start_flow(self, process_key, variables=None):
        '''
        开始流程，创建流程实例并绑定全局流程变量
        :param process_key: 流程图对应的key
        :param variables: 流程全局变量键值对
        :return:
        '''
        act_re_procdef = self.sql_class(db_name='act_re_procdef', cr=self.cr)
        procdef = act_re_procdef.select([], "key='{}'".format(process_key))
        assert len(procdef) == 1, 'there is no or beyond one procdef where key = "%s"' % process_key
        deployment_id = procdef[0]['deployment_id']
        tree_json = json.loads(procdef[0]['model_editor_json'])
        execution_id = uuid.uuid4()
        # 创建运行流程实例
        self.sql_class(db_name='act_run_execution', cr=self.cr).insert({
            'execution_id': execution_id, 'proc_inst_id': execution_id,
            'proc_def_id': deployment_id, 'act_id': None,
            'is_active': 1, 'is_concurrent': 0, 'is_scope': 0, 'parent_id': None,
            'is_event_scope': 0
        })
        elements_info = tree_json['elements_info']
        tag_name = elements_info[tree_json['start_element_key']]['tag_name']
        task_id = get_element_class(tag_name)(
            sql_class=self.sql_class, cr=self.cr, elements_info=elements_info,
            get_element_class_method=get_element_class
        ).start_task(tree_json['start_element_key'], execution_id, None)
        # 绑定全局变量
        if variables is not None:
            variables_values = get_variable_values(variables)
            for variable in variables_values:
                create_value = dict(variable, **{'execution_id': execution_id, 'proc_inst_id': execution_id})
                self.sql_class(db_name='act_ru_variable', cr=self.cr).insert(create_value)
        return {'execution_id': str(execution_id), 'task_id': str(task_id)}

    def _get_proc_record(self, task_id):
        '''
        在运行时任务表中根据task_id查询相应记录
        :param task_id:
        :return: (task_key, proc_record_info)
        '''
        run_task = self.sql_class(db_name='act_run_task', cr=self.cr).select([], "task_id='{}'".format(task_id))
        assert len(run_task) == 1, ValueError('there is no task where task_id is "{}"'.format(task_id))
        procdef_record = self.sql_class(db_name='act_re_procdef', cr=self.cr).select(
            [], "deployment_id='{}'".format(run_task[0]['proc_def_id']))
        return {'run_task': run_task, 'procdef_record': procdef_record}

    def complete_task(self, task_id, variables=None):
        '''
        完成当前任务
        :param task_id: 任务id
        :param variables: 变量
        :return:
        '''
        procdef = self._get_proc_record(task_id)
        task_key = procdef['run_task'][0]['task_key']
        elements_info = json.loads(procdef['procdef_record'][0]['model_editor_json'])['elements_info']
        element = get_element_class(elements_info[task_key]['tag_name'])
        # 获取解析节点的class，调用其中的complete_task执行完成节点操作
        element(sql_class=self.sql_class, cr=self.cr, elements_info=elements_info,
                get_element_class_method=get_element_class).complete_task(task_id, variables)

    @staticmethod
    def __get_users_or_groups_sql_word(_list, _type):
        user_or_group_sql = '''
            SELECT t.id FROM act_run_task as t INNER JOIN (
                SELECT m2m.task_id FROM act_run_task_m2m_relation AS m2m 
                INNER JOIN act_ru_variable AS va 
                ON m2m.variable_id=va.id WHERE 
                (m2m.is_variable=1 AND ({is_variable_where})) AND m2m.type='{type}'
                UNION 
                SELECT m2m.task_id FROM act_run_task_m2m_relation AS m2m WHERE 
                (m2m.is_variable=0 AND ({not_is_variable_where})) AND m2m.type='{type}'
            ) AS d ON t.task_id = d.task_id
        '''
        text_ = []
        long_ = []
        double_ = []
        for value in _list:
            if isinstance(value, str):
                text_.append(value)
            elif isinstance(value, int):
                long_.append(value)
            elif isinstance(value, float):
                double_.append(value)
            else:
                raise ValueError('the value of the list users/groups must be str or int or float!')
        is_variable_where_sql = ''
        not_is_variable_where_sql = ''
        _or = ''
        if len(text_) != 0:
            in_value = '(%s)' % str(text_)[1:-1]
            is_variable_where_sql = is_variable_where_sql + 'va.text_ IN {}'.format(in_value)
            not_is_variable_where_sql = not_is_variable_where_sql + 'm2m.text_ IN {}'.format(in_value)
            _or = ' OR '
        if len(long_) != 0:
            in_value = '(%s)' % str(long_)[1:-1]
            is_variable_where_sql = is_variable_where_sql + _or + 'va.long_ IN {}'.format(in_value)
            not_is_variable_where_sql = not_is_variable_where_sql + _or + 'm2m.long_ IN {}'.format(in_value)
            _or = ' OR '
        if len(double_) != 0:
            in_value = '(%s)' % str(double_)[1:-1]
            is_variable_where_sql = is_variable_where_sql + _or + 'va.double_ IN {}'.format(in_value)
            not_is_variable_where_sql = not_is_variable_where_sql + _or + 'm2m.double_ IN {}'.format(in_value)
        return user_or_group_sql.format(
            is_variable_where=is_variable_where_sql, not_is_variable_where=not_is_variable_where_sql, type=_type)

    def get_tasks(self, process_key=None, execution_id=None, proc_inst_id=None, task_id=None, users=None,
                          groups=None):
        '''
        获取当前进行的task任务
        :param process_key: str => 流程图的key
        :param execution_id: str => 初始实例id, 不是数据库id, 例: '3b435fc8-1f5b-42c2-a4db-c6724d767d1c'
        :param proc_inst_id: str => 当前实例id, 不是数据库id，同上
        :param task_id: str => 当前task_id, 不是数据库id，同上
        :param users: list => 节点user用户, 例: ['user1', 'user2']
        :param groups: list => 节点group组, 例: ['group1', 'group2']
        :return:
        '''
        if any([process_key, execution_id, proc_inst_id, task_id, users, groups]) is False:
            self.cr.execute('''SELECT * FROM act_run_task''')
            return self.cr.fetchall()
        sql_word = ''
        _and = ''
        if process_key is not None:
            act_re_procdef_record = self.sql_class(db_name='act_re_procdef', cr=self.cr).select(
                ['deployment_id'], "key='{}'".format(process_key))
            if len(act_re_procdef_record) == 0:
                return []
            else:
                sql_word += "proc_def_id='{}'".format(act_re_procdef_record[0]['deployment_id'])
                _and = ' and '
        if users is not None:
            user_or_group_sql = self.__get_users_or_groups_sql_word(users, 'user')
            self.cr.execute(user_or_group_sql)
            result = self.cr.fetchall()
            if len(result) == 0:
                return []
            else:
                in_value = '(%s)' % str([i[0] for i in result])[1:-1]
                sql_word = sql_word + _and + "id IN {}".format(in_value)
                _and = ' and '
        if groups is not None:
            user_or_group_sql = self.__get_users_or_groups_sql_word(groups, 'group')
            self.cr.execute(user_or_group_sql)
            result = self.cr.fetchall()
            if len(result) == 0:
                return []
            else:
                in_value = '(%s)' % str([i[0] for i in result])[1:-1]
                sql_word = sql_word + _and + "id IN {}".format(in_value)
                _and = ' and '
        if execution_id is not None:
            sql_word = sql_word + _and + "execution_id='{}'".format(execution_id)
            _and = ' and '
        if proc_inst_id is not None:
            sql_word = sql_word + _and + "proc_inst_id='{}'".format(proc_inst_id)
            _and = ' and '
        if task_id is not None:
            sql_word = sql_word + _and + "task_id='{}'".format(task_id)
            _and = ' and '
        return self.sql_class(db_name='act_run_task', cr=self.cr).select([], sql_word)

    def get_executions(self, process_key=None, execution_id=None, proc_inst_id=None):
        '''
        获取当前进行的流程实例
        :param process_key: str => 流程图的key
        :param execution_id: str => 初始实例id, 不是数据库id, 例: '3b435fc8-1f5b-42c2-a4db-c6724d767d1c'
        :param proc_inst_id: str => 当前实例id, 不是数据库id，同上
        :return:
        '''
        if any([process_key, execution_id, proc_inst_id]) is False:
            self.cr.execute('''SELECT * FROM act_run_execution''')
            return self.cr.fetchall()
        sql_word = ''
        _and = ''
        if process_key is not None:
            act_re_procdef_record = self.sql_class(db_name='act_re_procdef', cr=self.cr).select(
                ['deployment_id'], "key='{}'".format(process_key))
            if len(act_re_procdef_record) == 0:
                return []
            else:
                sql_word += "proc_def_id='{}'".format(act_re_procdef_record[0]['deployment_id'])
                _and = ' and '
        if execution_id is not None:
            sql_word = sql_word + _and + "execution_id='{}'".format(execution_id)
            _and = ' and '
        if proc_inst_id is not None:
            sql_word = sql_word + _and + "proc_inst_id='{}'".format(execution_id)
        return self.sql_class(db_name='act_run_execution', cr=self.cr).select([], sql_word)

    def change_variables(self, execution_id, variables):
        '''
        变更实例中的变量
        :param execution_id:
        :param variables: {'name': 'value'}
        :return:
        '''
        if variables is not None:
            variables_values = get_variable_values(variables)
            for variable in variables_values:
                create_value = dict(variable, **{'execution_id': execution_id, 'proc_inst_id': execution_id})
                self.sql_class(db_name='act_ru_variable', cr=self.cr).insert_or_update(
                    create_value,
                    "name='{}' and execution_id='{}'".format(variable['name'], execution_id)
                )

    def get_task_groups(self, task_id):
        '''
        根据task_id查询对应的groups
        :param task_id:
        :return:
        '''
        self.cr.execute('''SELECT t.task_key, p.model_editor_json, t.execution_id FROM act_run_task as t 
        LEFT JOIN act_re_procdef as p 
        ON t.proc_def_id = p.deployment_id 
        WHERE t.task_id = '%s';''' % task_id)
        result = self.cr.fetchone()
        if result is None:
            return []
        else:
            re_compile = re.compile(r'\$\((.*)\)')
            task_key = result[0]
            element_tree = json.loads(result[1])
            groups = element_tree['elements_info'][task_key]['properties'].get('candidateGroups', '')
            re_groups = re.match(re_compile, groups)
            if re_groups is not None:
                # groups是变量
                variables_groups = re_groups.groups()
                records = self.sql_class(db_name='act_ru_variable', cr=self.cr).select(
                    [], "execution_id='{}' and name in {}".format(
                        result[2], str(list(variables_groups)).replace('[', '(').replace(']', ')')
                    ))
                return list(get_variable_real_dict(records).values())
            else:
                return groups.split(',')