# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from xml.dom import minidom


from .base import Serializer
from ..parse import parse_element
from ..utils import join_tag_name


class XmlSerializer(Serializer):

    def __init__(self):
        self.start_tag_name = ''
        self.elements_info = {
            'process_id': None,
            'name': None,
            'version': None,
            'elements_info': {}
        }

    def deserialize_workflow_tree(self, file_content, filename=None):
        dom = minidom.parseString(file_content)
        self.start_tag_name = dom.childNodes[0].prefix
        process_dom = dom.getElementsByTagName(join_tag_name(self.start_tag_name, 'process'))
        assert len(process_dom) == 1, ValueError(
            'a bpmn file can only have one <{}> dom'.format(join_tag_name(self.start_tag_name, 'process')))
        # if startEvent's number > 1 raise
        start_event = process_dom[0].getElementsByTagName(join_tag_name(self.start_tag_name, 'startEvent'))
        assert len(start_event) == 1, ValueError(
            'a bpmn file can only have one <{}> dom'.format(join_tag_name(self.start_tag_name, 'startEvent')))

        for child_node in process_dom[0].childNodes:
            if child_node.nodeType == 3:  # 3 is the TEXT_NODE
                continue
            # parse element and return, write to database
            element = parse_element(self.start_tag_name, child_node, process_dom)
            # 将dom属性删除
            del element.process_dom
            serializer_element = element.serializer_element()
            # 将节点信息转化为json并附加在XmlSerializer类的属性里，返回当前实例
            self.elements_info['elements_info'][serializer_element['tag_id']] = serializer_element
        attributes = process_dom[0]._attrs
        for attr_key in attributes:
            if attributes[attr_key].localName == 'id':
                self.elements_info['process_id'] = attributes[attr_key].value
            elif attributes[attr_key].localName == 'name':
                self.elements_info['name'] = attributes[attr_key].value
            elif attributes[attr_key].localName == 'versionTag':
                self.elements_info['version'] = attributes[attr_key].value
        self.elements_info['start_element_key'] = start_event[0].getAttribute('id')
        return self


