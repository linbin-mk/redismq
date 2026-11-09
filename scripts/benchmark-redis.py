#!/usr/bin/env python3
"""只对显式指定的本机测试 Redis 压测；无第三方依赖，结束后精确删除随机队列键。

python3 scripts/benchmark-redis.py --port 16379 --output /tmp/redis-mq-benchmark.json
可用 --baseline /tmp/original-queue.lua 比较修改前脚本。只读查询 CONFIG，不修改配置，不执行 FLUSHDB 或 KEYS。
"""
import argparse
import hashlib
import json
import math
import socket
import time
import uuid
from pathlib import Path


class Redis:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.file = self.sock.makefile('rb')

    def call(self, *args):
        self.sock.sendall(self.encode(args))
        return self.read()

    @staticmethod
    def encode(args):
        parts = [f'*{len(args)}\r\n'.encode()]
        for value in args:
            value = value if isinstance(value, bytes) else str(value).encode()
            parts.extend((f'${len(value)}\r\n'.encode(), value, b'\r\n'))
        return b''.join(parts)

    def pipeline(self, commands):
        self.sock.sendall(b''.join(self.encode(command) for command in commands))
        return [self.read() for _ in commands]

    def read(self):
        marker = self.file.read(1)
        line = self.file.readline().rstrip(b'\r\n')
        if marker == b'+':
            return line.decode()
        if marker == b'-':
            raise RuntimeError(line.decode())
        if marker == b':':
            return int(line)
        if marker == b'$':
            size = int(line)
            if size < 0:
                return None
            value = self.file.read(size)
            if self.file.read(2) != b'\r\n':
                raise RuntimeError('RESP 数据损坏')
            return value
        if marker == b'*':
            return [self.read() for _ in range(int(line))]
        raise RuntimeError('Redis 连接已断开或响应无效')

    def close(self):
        self.file.close()
        self.sock.close()


def percentile(values, quantile):
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)]


def run(redis, script, label, size, messages):
    sha = redis.call('SCRIPT', 'LOAD', script).decode()
    tag = 'benchmark:' + str(uuid.uuid4())
    suffixes = ['scheduled', 'processing', 'messages', 'dead', 'dedup', 'dedup-expiry', 'wakeup', 'payloads', 'payload-bytes']
    keys = [f'rmq:{{{tag}}}:{suffix}' for suffix in suffixes]
    if label == 'baseline':
        keys = keys[:7]
    base = ['EVALSHA', sha, len(keys), *keys]
    retention = 86400000
    # 高转义内容也覆盖 JSON 编解码成本；恰好 size 个 UTF-8 字节。
    content = '\n"\\' * ((size - 2) // 6) + 'x' * ((size - 2) % 6)
    body = json.dumps(content, ensure_ascii=False).encode()
    assert len(body) == size
    fingerprint = hashlib.sha256(body).hexdigest()
    try:
        for begin in range(0, messages, 64):
            redis.pipeline([base + ['send', retention, f'm-{i:08}', body, fingerprint,
                                   0, 5, 10, 20, '', 86400000, messages + 1, 65536,
                                   128 * 1024 * 1024, messages * 3 + 100]
                            for i in range(begin, min(begin + 64, messages))])
        receive_ms, renew_ms, ack_ms, counts = [], [], [], []
        consume_started = time.perf_counter()
        for step in range(messages + 1):
            started = time.perf_counter_ns()
            batch = redis.call(*base, 'receive', retention, 100, 60000, str(step))
            receive_ms.append((time.perf_counter_ns() - started) / 1e6)
            if label == 'baseline':
                deliveries = json.loads(batch)['messages']
            else:
                deliveries = [json.loads(batch[i]) for i in range(4, len(batch), 2)]
            if not deliveries:
                break
            counts.append(len(deliveries))
            for delivery in deliveries:
                started = time.perf_counter_ns()
                assert redis.call(*base, 'renew', retention, delivery['id'], delivery['receipt'], 60000) == 1
                renew_ms.append((time.perf_counter_ns() - started) / 1e6)
                started = time.perf_counter_ns()
                assert redis.call(*base, 'ack', retention, delivery['id'], delivery['receipt'], '') == 1
                ack_ms.append((time.perf_counter_ns() - started) / 1e6)
        consume_seconds = time.perf_counter() - consume_started
        assert sum(counts) == messages
        server_samples = {'receive': [], 'renew': [], 'ack': []}
        # 读取慢日志，不清空共享日志；用随机队列键精确过滤本轮记录。
        for entry in redis.call('SLOWLOG', 'GET', 4096):
            command = entry[3]
            if len(command) > len(base) and command[3] == keys[0].encode():
                operation = command[len(base)].decode()
                if operation in server_samples:
                    server_samples[operation].append(entry[2] / 1000)
        result = {'version': label, 'payload_bytes': size, 'backlog': messages,
                  'delivered': sum(counts), 'consume_messages_per_second': round(messages / consume_seconds, 1), 'batch_max': max(counts), 'batch_mean': sum(counts) / len(counts)}
        for operation, values in [('receive', receive_ms), ('renew', renew_ms), ('ack', ack_ms)]:
            result[operation + '_roundtrip_ms'] = {
                'p50': round(percentile(values, .5), 3), 'p99': round(percentile(values, .99), 3),
                'max': round(max(values), 3)}
            samples = server_samples[operation]
            result[operation + '_slowlog'] = {'entries_ge_threshold': len(samples),
                                            'max_ms': round(max(samples, default=0), 3)}
        return result
    finally:
        redis.call('UNLINK', *keys)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    redis = Redis(args.port)
    try:
        config = redis.call('CONFIG', 'GET', 'slowlog-log-slower-than')
        report = {'server': redis.call('INFO', 'server').decode(),
                  'slowlog_threshold_us': int(config[1]), 'cases': []}
        scripts = [('optimized', Path(__file__).resolve().parents[1] / 'redis-mq-core/src/main/resources/com/mk/mq/queue.lua')]
        if args.baseline:
            scripts.insert(0, ('baseline', args.baseline))
        for label, path in scripts:
            for size, count in [(1024, 10000), (65536, 1000)]:
                result = run(redis, path.read_bytes(), label, size, count)
                report['cases'].append(result)
                print(json.dumps(result, ensure_ascii=False), flush=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    finally:
        redis.close()


if __name__ == '__main__':
    main()
