# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
import re
import datetime as dt
import uuid

from ..utils import get_variable_values, get_variable_real_dict


class ParseBase(object):
    def __init__(self, start_tag_name=None, process_dom=None, sql_class=None, cr=None, elements_info=None,
                 get_element_class_method=None):
        self.process_dom = process_dom
        self.start_tag_name = start_tag_name
        self.tag_name = None
        self.name = None
        self.tag_type = None
        self.tag_id = None
        self.properties = {}
        self.incoming = None  # the exclusiveGateway/task has this attr
        self.outgoing = None
        self.source = None  # only sequenceFlow has this attr
        # these attrs just in run work_flow time
        self.sql_class = sql_class
        self.cr = cr
        self.elements_info = elements_info  # xml节点的json内容
        self.get_element_class_method = get_element_class_method  # parse下的get_element_class方法

    @staticmethod
    def get_expression(expression):
        '''
        validate the expression
        :param expression:
        :return:
        '''
        variables = re.match(r'\$\((.*)\)', expression)
        if variables is None:
            raise ValueError('expression must be $(...)')
        else:
            # 因为psql存储字符串不能存单引号，故需要转成2个单引号
            return variables.groups()[0].replace('\'', '\'\'')

    def parse(self, element):
        '''
        :param element: xml element info
        :return:
        '''
        raise NotImplementedError("You must implement the deserialize_workflow_spec method.")

    def serializer_element(self):
        return self.__dict__

    def get_execution_variables(self, execution_id):
        '''
        从数据库读取现有变量
        :param execution_id:
        :return:
        '''
        records = self.sql_class(db_name='act_ru_variable', cr=self.cr).select(
            [], "execution_id='{}'".format(execution_id))
        return get_variable_real_dict(records)

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        '''
        开始一个新的task节点
        :param tag_id: 新节点的tag_id
        :param proc_inst_id: 流程实例id(即当前分支流程实例id)
        :param parent_task_id:
        :return:
        '''
        act_run_execution_record = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            [], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) == 1, ValueError(
            'there is no run_execution where proc_inst_id is "{}"'.format(proc_inst_id))
        # 开始创建task任务
        task_id = uuid.uuid4()
        self.sql_class(db_name='act_run_task', cr=self.cr).insert({
            'parent_task_id': parent_task_id,
            'task_id': task_id, 'task_key': tag_id, 'execution_id': act_run_execution_record[0]['execution_id'],
            'proc_inst_id': proc_inst_id, 'proc_def_id': act_run_execution_record[0]['proc_def_id'],
            'name': self.elements_info[tag_id]['name'], 'priority': 50
        })
        self.sql_class(db_name='act_run_execution', cr=self.cr).update(
            {'act_id': tag_id}, "proc_inst_id='{}'".format(proc_inst_id)
        )

    def complete_task(self, task_id, variables):
        '''
        完成当前task任务
        :param task_id: 当前正在运行的task_id
        :param variables: 传入的变量dict
        :param parent_task_id:
        :return:
        '''
        now_time = dt.datetime.now()
        run_task_sql_class = self.sql_class(db_name='act_run_task', cr=self.cr)
        run_task = run_task_sql_class.select([], "task_id='{}'".format(task_id))
        proc_inst_id = run_task[0]['proc_inst_id']
        # 完成当前任务, 查询任务的start_time
        assert len(run_task) == 1
        # 将任务存入历史记录
        run_task_sql_class.delete("task_id='{}'".format(run_task[0]['task_id']))
        self.sql_class(db_name='act_hi_taskinst', cr=self.cr).insert({
            'parent_task_id': task_id, 'task_id': run_task[0]['task_id'],
            'proc_def_id': run_task[0]['proc_def_id'], 'task_key': run_task[0]['task_key'],
            'proc_inst_id': run_task[0]['proc_inst_id'], 'execution_id': run_task[0]['execution_id'],
            'start_time': run_task[0]['start_time'], 'end_time': now_time,
            'name': run_task[0]['name'] or None
        })
        if variables is not None:
            variables_values = get_variable_values(variables)
            for variable in variables_values:
                create_value = dict(variable, **{'execution_id': run_task[0]['execution_id'],
                                                 'proc_inst_id': run_task[0]['proc_inst_id'],
                                                 'task_id': run_task[0]['task_id']})
                self.sql_class(db_name='act_ru_variable', cr=self.cr).insert_or_update(
                    create_value, "name='{}' and execution_id='{}'".format(variable['name'], run_task[0]['execution_id'])
                )
        # 创建下一个任务节点, 进入到相应的节点class调用一个开始方法
        outgoing = self.elements_info[run_task[0]['task_key']]['outgoing']
        self.get_element_class_method(self.elements_info[outgoing['tag_id']]['tag_name'])(
            sql_class=self.sql_class, cr=self.cr, elements_info=self.elements_info,
            get_element_class_method=self.get_element_class_method
        ).start_task(outgoing['tag_id'], proc_inst_id, task_id)
