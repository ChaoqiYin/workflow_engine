# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
import uuid
from ..base import ParseBase
from ...utils import join_tag_name


class ParseStartEvent(ParseBase):
    def parse(self, element):
        self.tag_name = element.localName
        self.name = element.getAttribute('name').replace('\'', '\"') or None
        self.tag_type = element.nodeType
        self.tag_id = element.getAttribute('id')
        self.properties = {}
        self.outgoing = {
            'tag_id': child.firstChild.data for child in
            element.getElementsByTagName(join_tag_name(self.start_tag_name, 'outgoing'))
        }
        return self

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        '''
        :param tag_id: 新节点的tag_id
        :param proc_inst_id: 因为是开始节点，所以这里此参数是初始实例id
        :param parent_task_id: None
        :return:
        '''
        act_run_execution_record = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            [], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) == 1, ValueError(
            'there is no run_execution where proc_inst_id is "{}"'.format(proc_inst_id))
        # 开始节点需要新建一个act_run_execution运行流程实例
        new_proc_inst_id = uuid.uuid4()
        self.sql_class(db_name='act_run_execution', cr=self.cr).insert({
            'execution_id': proc_inst_id, 'proc_inst_id': new_proc_inst_id,
            'proc_def_id': act_run_execution_record[0]['proc_def_id'], 'act_id': tag_id,
            'is_active': 1, 'is_concurrent': 0, 'is_scope': 0, 'parent_id': proc_inst_id,
            'is_event_scope': 0
        })
        # 开始创建task任务
        task_id = uuid.uuid4()
        self.sql_class(db_name='act_run_task', cr=self.cr).insert({
            'parent_task_id': parent_task_id,
            'task_id': task_id, 'task_key': tag_id, 'execution_id': proc_inst_id,
            'proc_inst_id': new_proc_inst_id, 'proc_def_id': act_run_execution_record[0]['proc_def_id'],
            'name': self.elements_info[tag_id]['name'], 'priority': 50
        })
        return task_id