# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from ..base import ParseBase
from ...utils import join_tag_name


class ParseEndEvent(ParseBase):

    def parse(self, element):
        self.tag_name = element.localName
        self.name = element.getAttribute('name').replace('\'', '\"') or None
        self.tag_type = element.nodeType
        self.tag_id = element.getAttribute('id')
        self.properties = {}
        return self

    def check_is_end(self, execution_id):
        records = self.sql_class(db_name='act_run_execution', cr=self.cr).select(
            ['id'], "execution_id='{}'".format(execution_id))
        return len(records) <= 1

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        self.complete_task(parent_task_id, proc_inst_id)
        return

    def complete_task(self, task_id, proc_inst_id):
        act_run_execution_sql_class = self.sql_class(db_name='act_run_execution', cr=self.cr)
        act_run_execution_record = act_run_execution_sql_class.select(
            ['execution_id'], "proc_inst_id='{}'".format(proc_inst_id))
        assert len(act_run_execution_record) != 0
        act_run_execution_sql_class.delete(
            "proc_inst_id='{}'".format(proc_inst_id)
        )
        # 检查是否所有实例都已结束，是的话则结束流程
        execution_id = act_run_execution_record[0]['execution_id']
        if self.check_is_end(execution_id):
            act_run_execution_sql_class.delete(
                "execution_id='{}'".format(execution_id)
            )
            self.sql_class(db_name='act_ru_variable', cr=self.cr).delete(
                "execution_id='{}'".format(execution_id)
            )
            self.sql_class(db_name='act_run_task_m2m_relation', cr=self.cr).delete(
                "execution_id='{}'".format(execution_id)
            )
        return
