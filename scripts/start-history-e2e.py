#!/usr/bin/env python3
"""启动仅存在于测试 classpath 的本机看板宿主；先运行带 mq.mysql/mq.redis 的 verify。"""
import argparse
import os
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--redis-url', default='redis://127.0.0.1:16379')
parser.add_argument('--mysql-url', required=True)
parser.add_argument('--port', type=int, default=18083)
parser.add_argument('--pid-file', type=Path)
args = parser.parse_args()
mysql = urlparse(args.mysql_url.removeprefix('jdbc:'))
redis = urlparse(args.redis_url)
if mysql.hostname not in ('127.0.0.1', 'localhost') or mysql.path != '/rmq_history_test' or redis.hostname not in ('127.0.0.1', 'localhost'):
    raise ValueError('只允许本机隔离 Redis 和 rmq_history_test 数据库')
if not 1024 <= args.port <= 65535:
    raise ValueError('测试端口超出范围')
root = Path(__file__).resolve().parents[1]
report = root / 'redis-mq-dashboard-mysql/target/surefire-reports/TEST-com.mk.mq.dashboard.mysql.HistoryWebE2ETest.xml'
if not report.exists():
    raise ValueError('请先运行包含真实 MySQL/Redis 参数的 mvn verify，生成测试宿主及 classpath')
properties = ET.parse(report).getroot().find('properties')
classpath = next(item.attrib['value'] for item in properties if item.attrib['name'] == 'java.class.path')
os.chdir(root)
if args.pid_file:
    args.pid_file.write_text(str(os.getpid()))
os.execvpe('java', ['java', '-Dmq.redis=' + args.redis_url, '-Dmq.mysql=' + args.mysql_url, '-cp', classpath,
                    'com.mk.mq.dashboard.mysql.HistoryE2EApplication', '--server.port=' + str(args.port)], os.environ)
