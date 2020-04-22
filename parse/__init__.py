# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from .base import ParseBase
from .event.start_event import ParseStartEvent
from .event.end_event import ParseEndEvent
from .sequence_flow import ParseSequenceFlow
from .gate_away import ParseExclusiveGateway, ParseParallelGateway
from .task import ParseTask, ParseUserTask


__registered_element = {
    'startEvent': ParseStartEvent,
    'sequenceFlow': ParseSequenceFlow,
    'exclusiveGateway': ParseExclusiveGateway,
    'parallelGateway': ParseParallelGateway,  # 并行网关
    'task': ParseTask,
    'userTask': ParseUserTask,
    'endEvent': ParseEndEvent
}


def parse_element(start_tag_name, element, process_dom):
    '''
    get the element's nodeName, and match the true parse class
    :param start_tag_name: bpmn's start tagName
    :param element: element dom in xml
    :param process_dom: xml project source dom
    :return:
    '''
    return __registered_element.get(element.localName, ParseBase)(start_tag_name, process_dom).parse(element)


def get_element_class(element_local_name):
    '''
    get the element's class to complete task or other db actions
    :param element_local_name:
    :return:
    '''
    return __registered_element.get(element_local_name, ParseBase)