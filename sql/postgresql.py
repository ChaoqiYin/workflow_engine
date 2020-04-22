# !user/bin/env python3
# -*- coding: utf-8 -*-
# Author: Artorias
from .base import BaseSql


class Posetgresql(BaseSql):
    def __init__(self, db_name, cr):
        self.db_name = db_name
        self._cr = cr

    @classmethod
    def db_exist(cls, cr):
        '''
        如果数据库不存在则新建数据库
        :param cls:
        :return: 
        '''
        cls.create_table_act_re_procdef(cr)
        cls.create_table_act_run_execution(cr)
        cls.create_table_act_run_task(cr)
        cls.create_table_act_ru_variable(cr)
        cls.create_table_act_run_task_m2m_relation(cr)
        cls.create_table_act_hi_taskinst(cr)
        cls.create_table_act_hi_actinst(cr)

    @staticmethod
    def create_table_act_re_procdef(cr):
        '''
        act_re_procdef: 部署流程表
            name: 流程的name
            key： 流程的标识
            version：流程的版本
            model_editor_json
            deployment_id： 部署的随机id,
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_re_procdef(
                id SERIAL PRIMARY KEY, name VARCHAR(255) NULL,
                key VARCHAR(255) NOT NULL, deployment_id VARCHAR(255) NOT NULL,
                version VARCHAR(255) NULL, model_editor_json TEXT NOT NULL,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT deployment_id_unique_a_b unique(deployment_id)
            );
            CREATE index IF NOT EXISTS idx_act_re_procdef_key on act_re_procdef(key)
        ''')

    @staticmethod
    def create_table_act_run_execution(cr):
        '''
        act_run_execution: 运行时流程实例表
            execution_id: 执行流程实例随机id(即初始流程实例的id)
            proc_inst_id: 所属流程实例id(即当前分支流程实例id)
            proc_def_id: 流程定义id
            parent_id: 父流程id
            act_id: 任务节点key, is_scope为1时act_id为Null
            is_active: 是否激活
            is_concurrent: 是否分支(并行)
            is_scope: 是否处于多实例或环节嵌套状态
            is_event_scope: 是否激活(待定)
            suspension_state: 挂起状态
            cached_ent_state: 缓存的状态
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_run_execution(
                id SERIAL PRIMARY KEY, execution_id VARCHAR(255) NOT NULL,
                proc_inst_id VARCHAR(255) NOT NULL, proc_def_id VARCHAR(255) NOT NULL,
                parent_id VARCHAR(255) NULL,
                act_id VARCHAR(255) NULL, is_active INTEGER NOT NULL,
                is_concurrent INTEGER NOT NULL, is_scope INTEGER NOT NULL,
                is_event_scope INTEGER NOT NULL, suspension_state VARCHAR(255) NULL,
                cached_ent_state INTEGER NULL,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT proc_inst_id_unique_a_b unique(proc_inst_id)
            );
            CREATE index IF NOT EXISTS idx_act_run_execution_proc_inst_id on act_run_execution(proc_inst_id);
            CREATE index IF NOT EXISTS idx_act_run_execution_proc_def_id on act_run_execution(proc_def_id)
        ''')

    @staticmethod
    def create_table_act_run_task(cr):
        '''
        act_run_task: 运行时任务表
            task_id: 任务的随机id
            task_key: 任务的key
            execution_id: 运行流程实例表(即初始流程实例的id)
            proc_inst_id: 流程实例id(即当前分支流程实例id)
            proc_def_id: 流程定义id
            name: 任务名称
            parent_task_id: 父任务id
            priority: 优先级, 默认50
            suspension_state: 挂起状态
            form_key: 任务内表单key
            start_time: 任务开始时间
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_run_task(
                id SERIAL PRIMARY KEY, task_id VARCHAR(255) NOT NULL,
                task_key VARCHAR(255) NOT NULL,
                execution_id VARCHAR(255) NOT NULL, proc_inst_id VARCHAR(255) NOT NULL,
                proc_def_id VARCHAR(255) NOT NULL, name VARCHAR(255) NULL,
                parent_task_id VARCHAR(255) NULL, priority INTEGER NOT NULL DEFAULT 50,
                suspension_state VARCHAR(255) NULL, form_key VARCHAR(255) NULL,
                start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT task_id_unique_a_b unique(task_id)
            );
            CREATE index IF NOT EXISTS idx_act_run_task_task_id on act_run_task(task_id)
        ''')

    @staticmethod
    def create_table_act_ru_variable(cr):
        '''
        act_ru_variable: 运行时流程变量
            type: 变量类型 double、integer、null、string,
            name: 变量名称,
            task_id: 节点id，
            execution_id: 初始执行实例id,
            proc_inst_id: 所属流程实例id,
            double_: 存储变量类型为浮点数的值
            long_: 存储变量类型为整数的值,
            text_: 存储变量类型为string的值
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_ru_variable(
                id SERIAL PRIMARY KEY, type VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL, execution_id VARCHAR(255) NOT NULL,
                proc_inst_id VARCHAR(255) NOT NULL, task_id VARCHAR(255) NULL, 
                double_ FLOAT8 NULL, long_ BIGINT NULL, text_ VARCHAR(4000) NULL,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE index IF NOT EXISTS idx_act_ru_variable_execution_id on act_ru_variable(execution_id)
        ''')

    @staticmethod
    def create_table_act_run_task_m2m_relation(cr):
        '''
        act_run_task_m2m_relation: 运行时task任务关联用户或组
            task_id: 任务id
            execution_id: 初始执行实例id,
            type: 用户user或者group
            variable_id: 对应变量值id
            is_variable: 是否为变量值
            variable_type: is_variable为0, 变量类型 double、integer、null、string,
            double_: is_variable为0, 存储变量类型为浮点数的值
            long_: is_variable为0, 存储变量类型为整数的值,
            text_: is_variable为0, 存储变量类型为string的值
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_run_task_m2m_relation(
                id SERIAL PRIMARY KEY, 
                execution_id VARCHAR(255) NOT NULL, 
                type VARCHAR(255) NOT NULL,
                task_id VARCHAR(255) NOT NULL,
                variable_id INTEGER NULL,
                is_variable INTEGER NOT NULL,
                variable_type VARCHAR(255) NULL,
                double_ FLOAT8 NULL, long_ BIGINT NULL, text_ VARCHAR(4000) NULL,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE index IF NOT EXISTS idx_act_run_task_m2m_relation_variable_id on act_run_task_m2m_relation(variable_id);
            CREATE index IF NOT EXISTS idx_act_run_task_m2m_relation_type on act_run_task_m2m_relation(type)
        ''')


    @staticmethod
    def create_table_act_hi_actinst(cr):
        '''
        act_hi_actinst: 历史的流程实例
            proc_inst_id: 所属流程实例id(即当前分支流程实例id)
            proc_def_id: 流程定义id
            act_id: 任务节点key,
            task_id: 任务实例ID 其他节点类型实例ID在这里为空
            act_name: 节点名称
            act_type: 节点类型
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_hi_actinst(
                id SERIAL PRIMARY KEY, 
                proc_def_id VARCHAR(255) NOT NULL, proc_inst_id VARCHAR(255) NOT NULL,
                act_id VARCHAR(255) NOT NULL, task_id VARCHAR(255) NULL,
                act_name VARCHAR(255) NULL, act_type VARCHAR(255) NOT NULL,
                start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE index IF NOT EXISTS idx_act_hi_taskinst_proc_def_id on act_hi_taskinst(proc_def_id)
        ''')

    @staticmethod
    def create_table_act_hi_taskinst(cr):
        '''
        act_hi_taskinst: 历史任务
            proc_def_id: 流程定义id,
            task_key: 任务的key,
            name: 任务名称,
            proc_inst_id: 所属流程实例id,
            execution_id: 执行实例id,
            task_id: 任务id,
            parent_task_id: 父任务id
            start_time: 任务开始时间,
            end_time: 任务结束时间,
            priority: 优先级, 默认50
        :param cr:
        :return:
        '''
        cr.execute('''
            CREATE TABLE IF NOT EXISTS act_hi_taskinst(
                id SERIAL PRIMARY KEY, proc_def_id VARCHAR(255) NOT NULL,
                task_key VARCHAR(255) NOT NULL, name VARCHAR(255) NULL, proc_inst_id VARCHAR(255) NULL,
                execution_id VARCHAR(255) NOT NULL, task_id VARCHAR(255) NOT NULL, parent_task_id VARCHAR(255) NULL,
                start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                priority INTEGER NOT NULL DEFAULT 50,
                create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE index IF NOT EXISTS idx_act_hi_taskinst_proc_def_id on act_hi_taskinst(proc_def_id)
        ''')

    def select_count(self, where=None):
        '''
        查询指定搜索记录的条数
        :param where:
        :return: int
        '''
        result = self.select(['COUNT(1)'], where)
        return result[0]['COUNT(1)']

    def select(self, cols, where=None):
        if isinstance(cols, list) and len(cols) == 0:
            cols = ['*']
        if where is None:
            sql = '''SELECT {} FROM {}'''.format(','.join(cols), self.db_name)
        else:
            sql = '''SELECT {} FROM {} WHERE {}'''.format(','.join(cols), self.db_name, where)
        self._cr.execute(sql)
        result = self._cr.fetchall()
        # 组装结果为键值对形式
        columns = [row[0] for row in self._cr.description] if cols == ['*'] else cols
        result = [[item for item in row] for row in result]
        return [dict(zip(columns, row)) for row in result]

    def update(self, set_value_dict, where=None):
        '''
        更新指定记录
        :param set_value_dict: {'key': 'value'}
        :param where:
        :return: int
        '''
        set_word = ''
        set_word_join = ''
        for key in set_value_dict:
            if isinstance(set_value_dict[key], int) or isinstance(set_value_dict[key], float):
                set_word = set_word + set_word_join + '{}={}'.format(key, set_value_dict[key])
            elif set_value_dict[key] is None:
                set_word = set_word + set_word_join + '{}=NULL'.format(key)
            else:
                set_word = set_word + set_word_join + "{}='{}'".format(key, set_value_dict[key])
            set_word_join = ','
        if where is None:
            sql = '''UPDATE {} SET {}'''.format(self.db_name, set_word)
        else:
            sql = '''UPDATE {} SET {} WHERE {}'''.format(self.db_name, set_word, where)
        self._cr.execute(sql)
        return self._cr.rowcount

    def insert(self, insert_value_dict):
        '''
        插入记录
        :param insert_value_dict: {col: value}
        :return:
        '''
        insert_keys = []
        insert_values = []
        for key in insert_value_dict:
            if isinstance(insert_value_dict[key], int) or isinstance(insert_value_dict[key], float):
                insert_keys.append(key)
                insert_values.append("{}".format(insert_value_dict[key]))
            elif insert_value_dict[key] is None:
                insert_keys.append(key)
                insert_values.append("NULL")
            else:
                insert_keys.append(key)
                insert_values.append("'{}'".format(insert_value_dict[key]))
        sql = '''INSERT INTO {} {} VALUES {}'''.format(
            self.db_name, '({})'.format(','.join(tuple(insert_keys))), '({})'.format(','.join(tuple(insert_values)))
        )
        self._cr.execute(sql)
        return self._cr.rowcount

    def insert_or_update(self, value_dict, where=None):
        if where is None or self.select_count(where) == 0:
            self.insert(value_dict)
        else:
            self.update(value_dict, where)

    def delete(self, where):
        '''
        删除记录
        :param where:
        :return: int
        '''
        sql = '''DELETE FROM {} WHERE {}'''.format(
            self.db_name, where
        )
        self._cr.execute(sql)
        return self._cr.rowcount
