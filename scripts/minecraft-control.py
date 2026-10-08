#!/usr/bin/env python3
"""Narrow owner-side Minecraft control over a private Unix socket.

Only root-configured server identifiers, UUID jobs, and prepare/resume actions
are accepted. No credentials, arbitrary paths, or shell commands cross the socket.
"""
import argparse
import contextlib
import io
import json
import os
import pathlib
import socket
import subprocess
import types
import uuid
import importlib.util
_spec=importlib.util.spec_from_file_location("minecraft_hook",pathlib.Path(__file__).with_name("minecraft-hook.py"))
minecraft_hook=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(minecraft_hook)


def serve(args):
    servers=json.loads(args.config.read_text())['servers']
    args.socket.parent.mkdir(mode=0o750,parents=True,exist_ok=True)
    if args.socket.exists():args.socket.unlink()
    listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    listener.bind(str(args.socket));os.chmod(args.socket,0o660);listener.listen(8)
    while True:
        connection,_=listener.accept()
        with connection:
            connection.settimeout(10);data=bytearray()
            while len(data)<=4096:
                chunk=connection.recv(4096)
                if not chunk:break
                data.extend(chunk)
            output=io.StringIO()
            try:
                request=json.loads(data)
                job=str(uuid.UUID(request['job_id']))
                server=servers[request['server']]
                phase=request['phase']
                if phase not in ('prepare','resume'):raise ValueError('unsupported action')
                os.environ['BACKUP_JOB_ID']=job
                state=args.state_dir/request['server']
                options=types.SimpleNamespace(server_dir=pathlib.Path(server['directory']),state_dir=state,
                    phase=phase,mcrcon=server.get('mcrcon','/usr/local/bin/mcrcon'),command_timeout=20,
                    simulate_save_failure=bool(request.get('simulate_save_failure',False)))
                with contextlib.redirect_stdout(output):minecraft_hook.run(options)
                if phase=='prepare':
                    # save-all can replace files with mode 0600. The owner grants
                    # the backup identity read access while saves are paused.
                    world=pathlib.Path(server['directory'])/server['world']
                    subprocess.run(['setfacl','-R','-P','-m','u:syncthing-backup:rX',str(world)],
                                   check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                result={'ok':True,'output':output.getvalue()}
            except Exception as error:
                result={'ok':False,'error':str(error),'output':output.getvalue()}
            connection.sendall(json.dumps(result).encode()+b'\n')


def request(args):
    payload={'server':args.server,'phase':args.phase,'job_id':os.environ.get('BACKUP_JOB_ID',''),
             'simulate_save_failure':args.simulate_save_failure}
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(100);connection.connect(str(args.socket));connection.sendall(json.dumps(payload).encode())
        connection.shutdown(socket.SHUT_WR);response=bytearray()
        while True:
            chunk=connection.recv(4096)
            if not chunk:break
            response.extend(chunk)
            if len(response)>65536:raise RuntimeError('oversized control response')
    result=json.loads(response)
    print(result.get('output',''),end='')
    if not result['ok']:raise RuntimeError(result.get('error','control operation failed'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['serve','request'])
    parser.add_argument('--socket',type=pathlib.Path,required=True)
    parser.add_argument('--config',type=pathlib.Path)
    parser.add_argument('--state-dir',type=pathlib.Path)
    parser.add_argument('--server')
    parser.add_argument('--phase',choices=['prepare','resume'])
    parser.add_argument('--simulate-save-failure',action='store_true')
    args=parser.parse_args()
    try:
        if args.mode=='serve':serve(args)
        else:request(args)
    except Exception as error:
        minecraft_hook.event('minecraft.control','failed',error=str(error));return 1
    return 0


if __name__=='__main__':
    import sys
    sys.exit(main())
