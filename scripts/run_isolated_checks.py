"""Start disposable local Mongo/API/worker processes and run synthetic checks.

Requires mongod on PATH and the installed api/dev extras. No Docker or existing
MongoDB volume is used. Ports and all data/log directories are newly allocated.
Run: python scripts/run_isolated_checks.py
"""
from __future__ import annotations

import os
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError('Isolated service did not become ready')


def main():
    mongod = shutil.which('mongod')
    if not mongod:
        raise SystemExit('Install mongod or use the documented isolated Compose replica set')
    processes, logs = [], []
    with tempfile.TemporaryDirectory(prefix='bharatpay-nav-check-') as directory:
        temporary = Path(directory)
        def start(name, command, environment=None):
            log = (temporary / (name + '.log')).open('w+')
            logs.append((name, log))
            process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            return process
        try:
            mongo_port, api_port = free_port(), free_port()
            mongo_data = temporary / 'mongo'
            mongo_data.mkdir()
            start('mongo', [mongod, '--dbpath', str(mongo_data), '--port', str(mongo_port),
                            '--bind_ip', '127.0.0.1', '--replSet', 'navtest'])
            direct = MongoClient(f'mongodb://127.0.0.1:{mongo_port}/?directConnection=true', serverSelectionTimeoutMS=300)
            try:
                wait_for(lambda: direct.admin.command('ping')['ok'])
                direct.admin.command('replSetInitiate', {'_id': 'navtest', 'members': [{'_id': 0, 'host': f'127.0.0.1:{mongo_port}'}]})
                wait_for(lambda: direct.admin.command('hello').get('isWritablePrimary'))
            finally:
                direct.close()
            url = f'mongodb://127.0.0.1:{mongo_port}/?replicaSet=navtest'
            environment = {**os.environ, 'MONGO_URL': url, 'MONGO_DATABASE': 'bharatpay_isolated_backend',
                           'NAV_TEST_MONGO_URL': url, 'API_BASE_URL': f'http://127.0.0.1:{api_port}',
                           'PYTHONUNBUFFERED': '1'}
            # Do not inherit source retention or quality overrides into fixtures.
            for key in list(environment):
                if key.startswith('NAV_') and key != 'NAV_TEST_MONGO_URL':
                    environment.pop(key)
            start('api', [sys.executable, '-m', 'uvicorn', 'bharatpay_mcp.api.app:app', '--host', '127.0.0.1', '--port', str(api_port)], environment)
            start('worker', [sys.executable, '-m', 'bharatpay_mcp.worker'], environment)
            def ready():
                with urllib.request.urlopen(environment['API_BASE_URL'] + '/ready', timeout=1) as response:
                    return response.status == 200
            wait_for(ready)
            subprocess.run([sys.executable, 'scripts/check_backend.py'], cwd=ROOT, env=environment, check=True)
            subprocess.run([sys.executable, '-m', 'pytest', 'integration/', '-q'], cwd=ROOT, env=environment, check=True)
            cli = subprocess.run([sys.executable, '-m', 'bharatpay_mcp.ingest', '--fixture', 'tests/fixtures/nav_synthetic.txt'],
                                 cwd=ROOT, env={**environment, 'MONGO_DATABASE': 'bharatpay_isolated_cli'},
                                 check=True, text=True, capture_output=True)
            result = json.loads(cli.stdout)
            assert result['status'] == 'completed' and result['trigger'] == 'cli' and result['upserted_count'] == 2
            print('PASS NAV: actual CLI fixture replay')
            standalone_port = free_port()
            standalone_data = temporary / 'standalone'
            standalone_data.mkdir()
            start('standalone', [mongod, '--dbpath', str(standalone_data), '--port', str(standalone_port), '--bind_ip', '127.0.0.1'])
            standalone_url = f'mongodb://127.0.0.1:{standalone_port}/'
            standalone = MongoClient(standalone_url, serverSelectionTimeoutMS=300)
            try:
                wait_for(lambda: standalone.admin.command('ping')['ok'])
            finally:
                standalone.close()
            subprocess.run([sys.executable, 'scripts/check_nav.py', '--standalone'], cwd=ROOT,
                           env={**environment, 'MONGO_URL': standalone_url}, check=True)
            print('PASS: isolated backend and NAV integration suites; existing volumes untouched')
        except BaseException:
            # Service logs contain synthetic data only; upstream source is mocked.
            for name, log in logs:
                log.flush()
                log.seek(0)
                print(name + ' diagnostics:', log.read()[-4000:])
            raise
        finally:
            for process in reversed(processes):
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for _, log in logs:
                log.close()


if __name__ == '__main__':
    main()
