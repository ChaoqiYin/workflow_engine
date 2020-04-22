# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from .base import ParseBase
from ..utils import join_tag_name


class ParseSequenceFlow(ParseBase):
    def __set_condition_expression(self, element):
        '''
        parse the expression for the sequenceFlow
        :param element:
        :return:
        '''
        condition_expression_dom = element.getElementsByTagName(
            join_tag_name(self.start_tag_name, 'conditionExpression'))
        if len(condition_expression_dom) == 1:
            self.condition_expression = self.get_expression(condition_expression_dom[0].firstChild.data)
        else:
            self.condition_expression = None

    def parse(self, element):
        self.tag_name = element.localName
        self.name = element.getAttribute('name').replace('\'', '\'\'') or None
        self.tag_type = element.nodeType
        self.tag_id = element.getAttribute('id')
        self.properties = {}
        self.outgoing = {
            'tag_id': element.getAttribute('targetRef')
        }
        self.source = {
            'tag_id': element.getAttribute('sourceRef')
        }
        self.__set_condition_expression(element)
        return self

    def start_task(self, tag_id, proc_inst_id, parent_task_id):
        # 子任务会在next出现，是一个另外的子任务节点start_task，并行会在include_gate_away的start_task时候出现多个流程
        self.start_next_task(tag_id, proc_inst_id, parent_task_id)

    def start_next_task(self, tag_id, proc_inst_id, parent_task_id):
        outgoing = self.elements_info[tag_id]['outgoing']
        self.get_element_class_method(self.elements_info[outgoing['tag_id']]['tag_name'])(
            sql_class=self.sql_class, cr=self.cr, elements_info=self.elements_info,
            get_element_class_method=self.get_element_class_method
        ).start_task(outgoing['tag_id'], proc_inst_id, parent_task_id)