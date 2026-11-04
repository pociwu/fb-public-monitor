"""OCI weekly Docker cleanup. Never touches application files or volumes."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess

APP = Path('/home/ubuntu/fb-public-monitor')
REPOSITORY = 'fb-public-monitor-monitor'


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def candidates(images, protected):
    versions = sorted((i for i in images if any(t.startswith(REPOSITORY + ':last-good-') for t in i.get('RepoTags') or [])),
                      key=lambda i: i['Created'], reverse=True)
    keep = {i['Id'] for i in versions[:2]} | protected
    return sorted(t for i in versions if i['Id'] not in keep
                  for t in i.get('RepoTags') or [] if t.startswith(REPOSITORY + ':last-good-'))


def notify(message):
    code = '''import os,sys,json,urllib.request
token=os.environ['TELEGRAM_BOT_TOKEN']; chat=os.environ['TELEGRAM_CHAT_ID']
body=json.dumps({'chat_id':chat,'text':sys.stdin.read()}).encode()
try:
 r=urllib.request.urlopen(urllib.request.Request('https://api.telegram.org/bot'+token+'/sendMessage',data=body,headers={'Content-Type':'application/json'}),timeout=25)
 print('telegram_ok',json.load(r).get('ok'))
except Exception as exc:
 print('telegram_failed',type(exc).__name__)
 sys.exit(1)
'''
    subprocess.run(['docker', 'compose', 'exec', '-T', 'monitor', 'python', '-c', code],
                   input=message, text=True, cwd=APP, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--notify', action='store_true')
    args = parser.parse_args()
    lock_path = Path.home() / 'oci-maintenance' / 'docker-cleanup.lock'
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        before = shutil.disk_usage('/').free
        results = []
        try:
            if (APP / 'deploy-state/deploy-maintenance').exists():
                results.append('部署維護中，本週略過清理。')
            else:
                ids = run('docker', 'ps', '-aq').split()
                containers = json.loads(run('docker', 'inspect', *ids)) if ids else []
                protected = {c['Image'] for c in containers}
                ids = run('docker', 'image', 'ls', '-q', '--no-trunc', REPOSITORY).split()
                images = json.loads(run('docker', 'image', 'inspect', *sorted(set(ids)))) if ids else []
                tags = candidates(images, protected)
                if not args.dry_run:
                    output = run('docker', 'builder', 'prune', '-af', '--filter', 'until=168h')
                    results.append(output.splitlines()[-1] if output else '建置快取清理完成')
                    for tag in tags:
                        # No force: Docker refuses deletion of images still used by containers.
                        run('docker', 'image', 'rm', tag)
                results.append(('預計移除' if args.dry_run else '已移除') + f' FB 舊映像 {len(tags)} 個')
            after = shutil.disk_usage('/').free
            results += [f'釋出空間：{max(0, after-before)/1024**3:.2f} GiB',
                        f'磁碟可用：{after/1024**3:.2f} GiB',
                        '保留現行及最近兩版回復映像；資料庫、媒體、登入資料、備份與 volumes 未清理。']
        except Exception as exc:
            results.append('清理未完成：' + type(exc).__name__)
            if args.notify:
                notify('【OCI 每週清理異常】\n' + '\n'.join(results))
            raise
        message = ('【OCI 每週清理排程驗證】每週日 04:30（台灣時間）\n' if args.dry_run else '【OCI 每週 Docker 清理】\n') + '\n'.join(results)
        print(message)
        if args.notify:
            notify(message)


if __name__ == '__main__':
    main()
