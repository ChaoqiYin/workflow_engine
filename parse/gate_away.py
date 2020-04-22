# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
import datetime as dt
import uuid
from .base import ParseBase
from ..utils import join_tag_name


class ParseGateAway(ParseBase):
    def parse(self, element):
        self.tag_name = element.localName
        self.name = element.getAttribute('name').replace('\'', '\"') or None
        self.tag_type = element.nodeType
        self.tag_id = element.getAttribute('id')
        self.properties = {}
        self.incoming = [
            {'tag_id': child.firstChild.data} for child in
            element.getElementsByTagName(join_tag_name(self.start_tag_name, 'incoming'))
        ]
        self.outgoing = [
            {'tag_id': child.firstChild.data} for child in
            element.getElementsByTagName(join_tag_name(self.start_tag_name, 'outgoing'))
        ]
        return self


class ParseExclusiveGateway(ParseGateAway):
    def parse(self, element):
        super().parse(element)
        self.default = element.getAttribute('default') or None
        return self

    def get_true_sequence_flow(self, tag_id, execution_id):
        '''
        获取满足判断条件的sequence_flow
        :param execution_id: 初始实例id
        :return: sequence_flow节点信息，返回的并不是实例
        '''
        proc_variables = self.get_execution_variables(execution_id)
        true_sequence_flows = []
        error_condition_expression = []
        for out_going_tag in self.elements_info[tag_id]['outgoing']:
            sequence_flow = self.elements_info[out_going_tag['tag_id']]
            # 比较判断式
            if sequence_flow['condition_expression'] is not None:
                try:
                    result = eval(sequence_flow['condition_expression'], proc_variables, proc_variables)
                    if result is True:
                        true_sequence_flows.append(sequence_flow['tag_id'])
                except NameError:
                    error_condition_expression.append(sequence_flow['condition_expression'])
                    continue
        length = len(true_sequence_flows)
        # 如果没有满足的情况则读取default
        if length == 0 and self.elements_info[tag_id]['default'] is not None:
            true_sequence_flows.append(self.elements_info[tag_id]['default'])
            length = len(true_sequence_flows)
        assert length == 1, 'the true of length of {} must is 1, now is {}'.format(error_condition_expression, length)
        return self.elements_info[true_sequence_flows[0]]

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        # 普通的判断节点不会出现多个并行的情况，会在ParallelGateway类中出现
        act_run_execution_record = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            [], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) == 1, ValueError(
            'there is no run_execution where proc_inst_id is "{}"'.format(proc_inst_id))
        self.sql_class(db_name='act_run_execution', cr=self.cr).update(
            {'act_id': tag_id}, "proc_inst_id='{}'".format(proc_inst_id)
        )
        self.complete_task(act_run_execution_record, tag_id, proc_inst_id, parent_task_id)

    def complete_task(self, act_run_execution_record, tag_id, proc_inst_id, parent_task_id):
        '''
        完成当前任务
        :param act_run_execution_record: 当前正在运行的流程实例
        :param tag_id: 传入的变量tag_id
        :param proc_inst_id: 当前分支流程实例id
        :param parent_task_id:
        :return:
        '''
        now_time = dt.datetime.now()
        task_id = uuid.uuid4()
        # 将任务存入历史记录
        self.sql_class(db_name='act_hi_taskinst', cr=self.cr).insert({
            'parent_task_id': parent_task_id, 'task_id': task_id,
            'proc_def_id': act_run_execution_record[0]['proc_def_id'], 'task_key': tag_id,
            'proc_inst_id': act_run_execution_record[0]['proc_inst_id'],
            'execution_id': act_run_execution_record[0]['execution_id'],
            'start_time': now_time, 'end_time': now_time, 'name': self.elements_info[tag_id]['name'] or None
        })
        # 创建下一个任务节点, 进入到相应的节点class调用一个开始方法
        true_sequence_flow = self.get_true_sequence_flow(tag_id, act_run_execution_record[0]['execution_id'])
        self.get_element_class_method(true_sequence_flow['tag_name'])(
            sql_class=self.sql_class, cr=self.cr, elements_info=self.elements_info,
            get_element_class_method=self.get_element_class_method
        ).start_task(true_sequence_flow['tag_id'], proc_inst_id, task_id)


class ParseParallelGateway(ParseGateAway):
    '''并行网关'''
    def judge_all_is_over(self, tag_id, proc_def_id):
        over_numbers = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            [], "act_id='sid-{}' and proc_def_id='{}'".format(tag_id, proc_def_id)
        )
        return len(over_numbers) == len(self.elements_info[tag_id]['incoming'])

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        '''
        判断是否满足所有并行条件，满足则删除所有支线，其余分支创建新实例，根据后续支线数量再创建相应的实例数，否则就将该实例的act_id变更为
        当前并行网关的tag_id的其他编码形式，即可通过select act_id=该编码 的数量来判断是否该并行网关的所有条件已达成
        '''
        act_run_execution_sql_class = self.sql_class(db_name='act_run_execution', cr=self.cr)
        act_run_execution_record = act_run_execution_sql_class.select(
            [], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) == 1, ValueError(
            'there is no run_execution where proc_inst_id is "{}"'.format(proc_inst_id))
        proc_def_id = act_run_execution_record[0]['proc_def_id']
        # 首先更改当前流程实例的状态，然后判断是否并行网关是否已满足下一步条件
        act_run_execution_sql_class.update(
            {'act_id': 'sid-{}'.format(tag_id)}, "proc_inst_id='{}'".format(proc_inst_id)
        )
        all_is_over = self.judge_all_is_over(tag_id, proc_def_id)
        if all_is_over is True:
            old_parent_id = act_run_execution_record[0]['parent_id']
            old_execution_id = act_run_execution_record[0]['execution_id']
            self.complete_task(tag_id, proc_def_id, old_execution_id, old_parent_id, parent_task_id)

    def complete_task(self, tag_id, proc_def_id, old_execution_id, old_parent_id, parent_task_id):
        act_run_execution_sql_class = self.sql_class(db_name='act_run_execution', cr=self.cr)
        # 删除前面所有流程实例
        act_run_execution_sql_class.delete("act_id='sid-{}' and proc_def_id='{}'".format(tag_id, proc_def_id))
        # TODO: 历史实例需要存入历史表
        # 将任务存入历史记录
        now_time = dt.datetime.now()
        task_id = uuid.uuid4()
        self.sql_class(db_name='act_hi_taskinst', cr=self.cr).insert({
            'parent_task_id': parent_task_id, 'task_id': task_id,
            'proc_def_id': proc_def_id, 'task_key': tag_id,
            'proc_inst_id': None,  # 并行有很多分支，实例不能唯一，故设为None
            'execution_id': old_execution_id,
            'start_time': now_time, 'end_time': now_time, 'name': self.elements_info[tag_id]['name'] or None
        })
        # 遍历并行分支创建新的run_task, 寻找并行分支的数量创建新的act_run_execution
        for sequence in self.elements_info[tag_id]['outgoing']:
            new_execution_id = uuid.uuid4()
            act_run_execution_sql_class.insert({
                'execution_id': old_execution_id, 'proc_inst_id': new_execution_id,
                'proc_def_id': proc_def_id, 'parent_id': old_parent_id,
                'act_id': None,  # 任务节点id先设为None，后续节点会更新此值
                'is_active': 1, 'is_concurrent': 1, 'is_scope': 0,
                'is_event_scope': 0
            })
            self.get_element_class_method(self.elements_info[sequence['tag_id']]['tag_name'])(
                sql_class=self.sql_class, cr=self.cr, elements_info=self.elements_info,
                get_element_class_method=self.get_element_class_method
            ).start_task(sequence['tag_id'], new_execution_id, parent_task_id)


class ParseInclusiveGateWay(ParseGateAway):
    pass


class ParseComplexGateWay(ParseGateAway):
    pass


class ParseEventBasedGateWay(ParseGateAway):
    pass
