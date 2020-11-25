# -*- coding:utf-8 -*-

import cat

from base.log import logger
from base.singleton import singleton

@singleton
class CatInit(object):
    def __init__(self):
        logger().info('cat init')
        cat.init("kd.hbase")

@singleton
class CatStage(object):
    def __init__(self):
        CatInit()
        self.__stage_map = {}

    def stage_begin(self, space, name):
        key = (space, name)
        name = '%s/%s' % (space, name)
        self.__stage_map[key] = cat.Transaction(space, name)

    def stage_end(self, space, name):
        key = (space, name)
        assert key in self.__stage_map
        self.__stage_map[key].complete()
        del self.__stage_map[key]

@singleton
class CatStageAgency(object):
    def __init__(self):
        CatInit()
        self.reset()

    def reset(self):
        self.__stage_list = []

    def stage_begin(self, space, name):
        key = (space, name)
        name = '%s/%s' % (space, name)
        tran = cat.Transaction(space, name)
        self.__stage_list.append(tran)

    def finish(self):
        if len(self.__stage_list) > 0:
            logger().info('cat finish, flush %d transactions',
                          len(self.__stage_list))
        for val in self.__stage_list[::-1]:
            val.complete()
        self.__stage_list = []

#----------------Shortcut--------------

def stage_begin(space, name):
    CatStage().stage_begin(space, name)

def stage_end(space, name):
    CatStage().stage_end(space, name)

def agent_stage_begin(space, name):
    CatStageAgency().stage_begin(space, name)

def agent_reset():
    CatStageAgency().reset()

def agent_finish():
    CatStageAgency().finish()

def log_event(space, name):
    name = '%s/%s' % (space, name)
    cat.log_event(space, name)

def test():
    import time
    stage_begin('a', '3')
    stage_begin('a', '4')
    time.sleep(1)
    stage_end('a', '4')
    stage_end('a', '3')

def test2():
    import time
    agent_stage_begin('b', 'b')
    agent_stage_begin('b', 'c')
    agent_stage_begin('b', 'd')
    time.sleep(1)
    agent_finish()

def test1():
    import time
    cat.init("kd.test")
    a = cat.Transaction('c', 'b')
    time.sleep(2)
    a.complete()
    b = cat.Transaction('c', 'c')
    time.sleep(2)
    b.complete()

def main():
    test2()

if __name__ == '__main__':
    main()




