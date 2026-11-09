#!/usr/bin/env python3
"""仅用于本机 HistoryE2EApplication：实际停止/启动专用 MySQL，验证 MQ 隔离与历史恢复。"""
import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
from urllib.error import HTTPError, URLError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:18083')
    parser.add_argument('--mysql-container', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    parsed = urlparse(args.base_url)
    if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost'):
        raise ValueError('E2E 仅允许本机 HTTP 地址')
    if not args.mysql_container.startswith('redis-mq-history-'):
        raise ValueError('只允许停止本轮 redis-mq-history-* 测试容器')
    endpoint = os.environ.get('DOCKER_HOST') or subprocess.check_output(
        ['docker', 'context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'], text=True).strip()
    if not endpoint.startswith(('unix://', 'npipe://')):
        raise ValueError('禁止对远程 Docker 执行本机 E2E 停机验证')
    inspected = json.loads(subprocess.check_output(['docker', 'inspect', args.mysql_container]))[0]
    if 'MYSQL_DATABASE=rmq_history_test' not in inspected['Config']['Env']:
        raise ValueError('目标不是指定的隔离 E2E 数据库')

    def request(path):
        try:
            with urlopen(args.base_url.rstrip('/') + path, timeout=5) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def get(path):
        status, result = request(path)
        if status != 200:
            raise AssertionError(f'{path}: HTTP {status}')
        return result

    def wait(condition, seconds=50):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                result = condition()
                if result:
                    return result
            except (OSError, URLError, AssertionError):
                pass
            time.sleep(.25)
        raise AssertionError('等待 E2E 条件超时')

    wait(lambda: get('/redis-mq/dashboard/api/history/status')['state'] == 'RUNNING')
    before = get('/fixture/status')
    assert before['synthetic'] is True
    start = int(time.time() * 1000)
    window = start // 60000 * 60000
    history_path = f'/redis-mq/dashboard/api/queues/analysis-demo/history?from={window}&points=100'
    previous = get(history_path)
    before_ack = previous['summary']['counts']['acknowledged'] if previous['summary'] else 0
    report = {'synthetic_fixture': True, 'started_at': start, 'before': before, 'before_acknowledged': before_ack}
    stopped = False
    try:
        subprocess.run(['docker', 'stop', '-t', '5', args.mysql_container], check=True, stdout=subprocess.DEVNULL)
        stopped = True
        wait(lambda: get('/redis-mq/dashboard/api/history/status')['state'] == 'DEGRADED')
        time.sleep(4)
        during = get('/fixture/status')
        assert during['accepted'] > before['accepted'] + 5
        assert during['succeeded'] > before['succeeded'] + 5, 'MySQL 停机时 MQ 未继续完成消费'
        status, _ = request(history_path)
        assert status == 503, '数据库停机不应返回伪造的成功历史数据'
        report['during_mysql_down'] = during
        report['history_http_during_outage'] = status
    finally:
        if stopped:
            subprocess.run(['docker', 'start', args.mysql_container], check=True, stdout=subprocess.DEVNULL)
    wait(lambda: get('/redis-mq/dashboard/api/history/status')['state'] == 'RUNNING', seconds=60)
    restored = wait(lambda: (lambda r: r if r['summary'] and r['summary']['counts']['acknowledged'] > before_ack + 5 else None)(get(history_path)))
    retention = get('/redis-mq/dashboard/api/queues/analysis-demo/history?from=1&points=17')
    assert len(retention['points']) <= 17
    assert retention['to'] - retention['from'] <= 5 * 86400000
    report.update({'after': get('/fixture/status'), 'after_acknowledged': restored['summary']['counts']['acknowledged'],
                   'history_status': get('/redis-mq/dashboard/api/history/status'),
                   'retention_points': len(retention['points']), 'finished_at': int(time.time() * 1000), 'passed': True})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'passed': True, 'accepted_while_mysql_down': report['during_mysql_down']['accepted'] - before['accepted'],
                      'succeeded_while_mysql_down': report['during_mysql_down']['succeeded'] - before['succeeded'],
                      'history_recovered': True, 'output': str(args.output)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
