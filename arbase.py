# -*- coding: utf-8 -*- 
import  platform
import logging
import logging.handlers
import json
import os
import subprocess
# import string
import socket
import time
import sqlite3
import argparse

#import cx_Oracle
# from StringIO import StringIO
# python3 from io import StringIO 

class ARConfig:
    def __formatARTime(self, tmstr):
        """格式化AR的时间戳"""
        try:
            a = str(tmstr)
            a = a[:len(a) - 6]
            a = int(a)
            # a = string.atol(a)
            r = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(a))
        except Exception as e:
            r =None
        return r

    def __initLog(self):
        '''初始化日志'''
        logdir = os.path.join(self.__scriptPath, 'log')
        if not os.path.exists(logdir):
            os.mkdir(logdir)
        nm = '%s_%s.%s' %  ('replicatecommand',
             time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())),
             'log')
        lnm = os.path.join(logdir, nm)

        """INIT LOG SETTING,初始化日志设置"""
        logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        logging.basicConfig(level=logging.INFO, format=logformat,
                            datefmt='%Y-%m-%dT%H:%M:%S',
                            filename=lnm)
        self.log = logging.getLogger('replicatecommand')

    def __readconfig(self,jsf):
        '''读取配置文件获取自定义设置'''
        if os.path.exists(jsf):
            f = open(jsf)
            try:
                cfg = json.load(f)
                return cfg
            except Exception as e:
                print(str(e))
                return None
        else:
            print('config file %s is not found'% (jsf))
            return None

    def __init__(self,configfile=''):
        self.__scriptPath=os.path.split(os.path.realpath(__file__))[0]
        self.__initLog()

        if platform.system()=='Windows':
            self.replicateHome = r'C:\\Program Files\\Attunity\\Replicate'
            # self.bin_dir=r'C:\\Program Files\\Attunity\\Replicate\\bin'
            self.data_dir=r'C:\\Program Files\\Attunity\\Replicate\\data'
            self.commandline='repctl.exe'
        else:
            self.replicateHome = '/opt/attunity/replicate'
            # self.bin_dir='/opt/attunity/replicate/bin'
            self.data_dir='/opt/attunity/replicate/data'
            self.commandline='repctl'
        # 使用自定配置覆盖
        if configfile:
            cfg = self.__readconfig(configfile)
            if cfg:
                if 'home_dir' in cfg.keys():
                    self.replicateHome = cfg['home_dir']
                if 'data_dir' in cfg.keys():
                    self.data_dir = cfg['data_dir']
        self.bin_dir = os.path.join(self.replicateHome,'bin')

        self.log.info('replicate bin set : %s' % (self.bin_dir))
        self.log.info('replicate data set : %s' % (self.data_dir))
        self.command={
            'gettasklist': "connect\ngettasklist %\nquit\n",
            'gettaskstatus': 'connect\ngettaskstatus "%s"\nquit\n',
            'stoptask': 'connect\nstoptask %s\nquit\n',
            'resume' : 'connect\nexecute %s operation=3 flags=0\nquit\n',
            'reload' : 'connect\nexecute %s operation=3 flags=1\nquit\n',
            'gettablesstatus' : 'connect\ngettablesstatus task=%s mask=127 last_update_time=0\nquit\n'
        }
        self.dbmapping={
                'audit':'task_audit.sqlite',
                'tables':'task_tables.sqlite'
                }
        self.hostname = socket.gethostname()
        self.execute = os.path.join(self.bin_dir, self.commandline)

    def _gettaskdb(self, taskname, dbtype):
        fnm = os.path.join(self.data_dir,'tasks',taskname,self.dbmapping[dbtype])
        if os.path.exists(fnm):
            return fnm
        else:
            self.log.error('%s is not found'%(fnm))
            return None

    def gettablestatus(self, taskname):
        '''从sqlite中获取ttable状态'''
        dbfile = self._gettaskdb(taskname,'tables')
        conn = sqlite3.connect(dbfile)
        sql = 'select owner,name,table_status,start_time,end_time from tables_status'
        cur = conn.execute(sql)
        for r in cur.fetchall():
            print(r[0],r[1],r[2],self.__formatARTime(r[3]),
                    self.__formatARTime(r[4]))

    def executeARCmd(self,cmd):
        '''执行ar命令行'''
        try:
            f = open('./execute_out.txt','w')
            logging.info('execute "%s" %s "%s"' % (self.execute,'-d',self.data_dir))
            # 执行ar command，添加环境变量
            if platform.system()=='Windows':
                arenv= None
            else:
                # linux平台需要设置环境变量
                arenv = {"PATH":"$PATH:%s"%(
                                      os.path.join(self.replicateHome,'bin')),
                                      "LD_LIBRARY_PATH":"$LD_LIBRARY_PATH:%s"%(
                                          os.path.join(self.replicateHome,'lib'))
                                  }
            p = subprocess.Popen([self.execute,'-d',self.data_dir],
                                 env=arenv,
                                 stdin=subprocess.PIPE,
                                 stdout=f,
                                 universal_newlines=True)
            
            p.stdin.write(cmd)
            p.stdin.flush()
            p.wait()
            f.flush()
            f.close()
        except Exception as e:
            logging.error(str(e)) 

    def parseArout(self):
        """解析AR命令行返回的文件为json格式"""
        b = ''
        start = False
        f = open('./execute_out.txt','r')
        while 1:
            si = f.readline()
            if not si:
                break
            if (si[0] == '{'):
                start = True
            if start:
                b = b + si
                if (si[0] == '}'):
                    break
        try:
            xx = json.loads(b)
        except Exception as e:
            logging.error(str(e))
        return xx

    def getTaskList(self):
        """
        return dataformat
        {
            "task_desc_list": [
                {
                    "name": "New Task 1",
                    "source_name": "s_o",
                    "target_names": [
                        "t_oracle"
                    ]
                },
                {
                    "name": "db2-oracle",
                    "source_name": "s_db2",
                    "target_names": [
                        "t_oracle"
                    ]
                },
                {
                    "name": "my-o",
                    "source_name": "s_lmysql",
                    "target_names": [
                        "t_oracle"
                    ]
                },
                {
                    "name": "p-o",
                    "source_name": "s_lpos",
                    "target_names": [
                        "t_oracle"
                    ]
                }
            ]
        }
        """
        self.executeARCmd(self.command['gettasklist'])
        return self.parseArout()

    def getTaskStatus(self,taskname):
        """
        {
            "task_status": {
                "starting_state": "AR_STATE_STARTING",
                "source_status": {},
                "fresh_start_time": 1571230636156586,
                "name": "t1",
                "running_instance": {
                    "instance_uuid": "931a70fc-d70c-4d11-888c-60784bed032c",
                    "instance_uuid_int_h": 1246666793644202643,
                    "task_uuid": "52fe0fe0-c52d-49ee-a36b-775d786cb914",
                    "instance_uuid_int_l": 3171639470996884616
                },
                "cdc_start_position": "00000000.003383b0.00000001.0006.00.0000: 79.1301.16",
                "cdc_start_id": 685,
                "cdc_latency": {},
                "full_load_start_time": 1571230639583984,
                "full_load_throughput": {},
                "metadata_last_timestamp": 1571230641833033,
                "stop_time": 1571230816182961,
                "state": "STOPPED",
                "full_load_counters": {
                    "full_load_total_record_transferred": 5,
                    "full_load_canceled_tables_count": 0,
                    "full_load_completed_tables_count": 6,
                    "full_load_estimated_records_count_for_all_tables": 3,
                    "full_load_loading_tables_count": 0,
                    "full_load_queued_tables_count": 0,
                    "full_load_error_tables_count": 0
                },
                "full_load_finish_time": 1571230642018195,
                "cdc_transactions_counters": {
                    "applied_swaped_events": 0,
                    "read_volume_in_progress": 0,
                    "read_records_rollback": 0,
                    "read_rollback": 0,
                    "read_volume_committed": 0,
                    "read_in_progress": 0,
                    "read_committed": 0,
                    "applied_records_committed": 0,
                    "read_volume_rollback": 0,
                    "read_swaped_events": 0,
                    "applied_volume_committed": 0,
                    "applied_in_progress": 0,
                    "applied_volume_in_progress": 0,
                    "applied_memory_events": 0,
                    "read_records_in_progress": 0,
                    "read_memory_events": 0,
                    "applied_committed": 0,
                    "read_records_committed": 0,
                    "applied_records_in_progress": 0
                },
                "full_load_completed": True,
                "target_status": {},
                "start_time": 1571230636156586,
                "task_cdc_event_counters": {
                    "cdc_read_ddl_count": 0,
                    "cdc_applied_insert_count": 0,
                    "cdc_read_delete_count": 0,
                    "cdc_read_update_count": 0,
                    "cdc_filtered_insert_count": 0,
                    "cdc_applied_ddl_count": 0,
                    "cdc_read_insert_count": 0,
                    "cdc_filtered_ddl_count": 0,
                    "cdc_filtered_delete_count": 0,
                    "cdc_filtered_update_count": 0,
                    "cdc_applied_delete_count": 0,
                    "cdc_applied_update_count": 0
                },
                "cdc_throughput": {
                    "source_throughput_volume": {},
                    "target_throughput_records": {},
                    "target_throughput_volume": {},
                    "network_throughput_volume": {},
                    "source_throughput_records": {},
                    "network_throughput_records": {}
                }
            }
        }
        """
        self.executeARCmd(self.command['gettaskstatus'] % (taskname))
        return self.parseArout()

    def resumetask(self,taskname):
        self.executeARCmd(self.command['resume'] % (taskname))
        return self.parseArout()

    def reloadtask(self,taskname):
        self.executeARCmd(self.command['reload'] % (taskname))
        return self.parseArout()

    def stoptask(self,taskname):
        self.executeARCmd(self.command['stoptask'] % (taskname))
        return self.parseArout()

    def gettabledetail(self,taskname):
        self.executeARCmd(self.command['gettablesstatus'] % (taskname))
        return self.parseArout()

    def getTaskInfoByPendding(self, count=1, waits=10):
        i = 0
        for i in range(count):
            tlist = self.getTaskList()
            for t in tlist['task_desc_list']:
                tst = t
                c = self.getTaskStatus(t['name'])
                tst['status']=c['task_status']
                if 'full_load_start_time' in tst['status'].keys():
                    tst['status']['full_load_start_time'] = self.__formatARTime(
                            tst['status']['full_load_start_time'])
                if 'full_load_finish_time' in tst['status'].keys():
                    tst['status']['full_load_finish_time'] = self.__formatARTime(
                            tst['status']['full_load_finish_time'])

                if 'start_time' in tst['status'].keys():
                    tst['status']['start_time'] = self.__formatARTime(
                            tst['status']['start_time'])
                if 'fresh_start_time' in tst['status'].keys():
                    tst['status']['fresh_start_time'] = self.__formatARTime(
                            tst['status']['fresh_start_time'])
                if 'metadata_last_timestamp' in tst['status'].keys():
                    tst['status']['metadata_last_timestamp'] = self.__formatARTime(
                            tst['status']['metadata_last_timestamp'])
                if 'stop_time' in tst['status'].keys():
                    tst['status']['stop_time'] = self.__formatARTime(
                        tst['status']['stop_time'])
                # add @timestamp
                tst['@timestamp']= time.strftime("%Y-%m-%dT%H:%M:%S",
                        time.localtime(time.time()))
                # add hostname
                tst['hostname'] = self.hostname
                # add index type
                tst['type']='armon'
                # write data to ar and type is armon(history)
                self.write2es('ar',tst)
                
                # change type to now mon
                tst['type'] = 'nowmon'
                # updata ar index by id
                self.write2es('ar',tst,iid=tst['hostname']+'_'+tst['name'])

            time.sleep(waits)


def showconfig(args):
    f = open(args.config)
    cfg = json.load(f)
    print(cfg)
    f.close()

def resumetask(args):
    ar = ARConfig(args.config)
    r = ar.resumetask(args.task)
    print(r)

def reloadtask(args):
    ar = ARConfig(args.config)
    r = ar.reloadtask(args.task)
    print(r)

def stoptask(args):
    ar = ARConfig(args.config)
    r = ar.stoptask(args.task)
    print(r)

def gettaskstatus(args):
    ar = ARConfig(args.config)
    r = ar.getTaskStatus(args.task)
    print(r)
    
def gettablestatus(args):
    ar = ARConfig(args.config)
    if args.detail:
        r = ar.gettabledetail(args.task)
        print(r)
    else:
        r = ar.gettablestatus(args.task)

def main():
    # create the top-level parser
    # 公共连接参数，并设置默认值
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--config', default="", help='attunity Replicate config file')
    # parser.add_argument('-o','--outputfile', default="search.csv", help='Output File ')
    #parser.add_argument('-l','--headline', action='store_true', help='output file include   head line')
    # parser.add_argument('-l','--headline', default="", help='output file include head line'  )

    #添加子命令
    subparsers = parser.add_subparsers(title="sub command",description="show base config",help='sub-command help')

    # create the parser for the "action" command
    parser_a = subparsers.add_parser('showconfig', help='show base config')
    parser_a.set_defaults(func=showconfig)

    parser_b = subparsers.add_parser('resume', help='show base config')
    parser_b.add_argument('-t', '--task', default='', help='taskname')
    parser_b.set_defaults(func=resumetask)

    parser_c = subparsers.add_parser('stoptask', help='show base config')
    parser_c.add_argument('-t', '--task', default='', help='taskname')
    parser_c.set_defaults(func=stoptask)

    parser_d = subparsers.add_parser('reload', help='show base config')
    parser_d.add_argument('-t', '--task', default='', help='taskname')
    parser_d.set_defaults(func=reloadtask)

    parser_e = subparsers.add_parser('gettaskstatus', help='show base config')
    parser_e.add_argument('-t', '--task', default='', help='taskname')
    parser_e.set_defaults(func=gettaskstatus)

    parser_f = subparsers.add_parser('gettablestatus', help='show base config')
    parser_f.add_argument('-t', '--task', default='', help='taskname')
    parser_f.add_argument('-d', '--detail',action='store_true',  help='detail message')
    parser_f.set_defaults(func=gettablestatus)

    parser_g = subparsers.add_parser('gettabledetail', help='show base config')
    parser_g.add_argument('-t', '--task', default='', help='taskname')
    parser_g.set_defaults(func=gettablestatus)
    #get agg 设置
    """
    parser_b = subparsers.add_parser('getagg', help='export help')
    parser_b.add_argument('-g', '--glist',required=True, default=None, help='group by field   list;split by ,')
    parser_b.add_argument('-d', '--dlist',required=True, default=None, help='value by field   list;split by ,')
    parser_b.add_argument('-p', '--prefix', default='', help='data field prefix,default is   None space')
    parser_b.add_argument('-c', '--count',action='store_true',  help='get doc_count, not de  f get value')
    parser_b.set_defaults(func=aggJson2csv)
    """

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
