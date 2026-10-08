import importlib.util
import json
import os
import pathlib
import socket
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

PATH=pathlib.Path(__file__).resolve().parents[1]/'scripts/minecraft-hook.py'
spec=importlib.util.spec_from_file_location('minecraft_hook',PATH)
hook=importlib.util.module_from_spec(spec);spec.loader.exec_module(hook)
control_spec=importlib.util.spec_from_file_location('minecraft_control',PATH.with_name('minecraft-control.py'))
control=importlib.util.module_from_spec(control_spec);control_spec.loader.exec_module(control)

class MinecraftHooks(unittest.TestCase):
    def test_repeated_prepare_preserves_its_obligation_to_resume(self):
        with tempfile.TemporaryDirectory() as root:
            root=pathlib.Path(root);server=root/'server';server.mkdir();state=root/'leases'
            (server/'server.properties').write_text('enable-rcon=true\nrcon.password=not-logged\n')
            args=types.SimpleNamespace(server_dir=server,state_dir=state,phase='prepare',mcrcon='/bin/true',command_timeout=1,simulate_save_failure=False)
            replies=[types.SimpleNamespace(returncode=0,stdout=text) for text in (
                'Automatic saving is now disabled','Saved the game',
                'Saving is already turned off','Saved the game','Automatic saving is now enabled')]
            with patch.dict(os.environ,{'BACKUP_JOB_ID':str(uuid.uuid4())}),patch.object(hook.subprocess,'run',side_effect=replies) as command:
                hook.run(args);hook.run(args)
                self.assertTrue(json.loads(next(state.glob('*.json')).read_text())['resume'])
                args.phase='resume';hook.run(args)
                self.assertEqual(command.call_count,5)
                self.assertEqual(list(state.glob('*.json')),[])
    def test_protocol_success_with_rejected_save_keeps_recoverable_lease(self):
        with tempfile.TemporaryDirectory() as root:
            root=pathlib.Path(root);server=root/'server';server.mkdir();state=root/'leases'
            (server/'server.properties').write_text('enable-rcon=true\nrcon.port=25575\nrcon.password=not-logged\n')
            args=types.SimpleNamespace(server_dir=server,state_dir=state,phase='prepare',mcrcon='/bin/true',command_timeout=1,simulate_save_failure=False)
            replies=[types.SimpleNamespace(returncode=0,stdout='Automatic saving is now disabled'),types.SimpleNamespace(returncode=0,stdout='Unknown command')]
            with patch.dict(os.environ,{'BACKUP_JOB_ID':str(uuid.uuid4())}),patch.object(hook.subprocess,'run',side_effect=replies):
                with self.assertRaisesRegex(RuntimeError,'acknowledge'):hook.run(args)
                self.assertEqual(len(list(state.glob('*.json'))),1)
                args.phase='resume'
                with patch.object(hook.subprocess,'run',return_value=types.SimpleNamespace(returncode=0,stdout='Automatic saving is now enabled')):hook.run(args)
                self.assertEqual(list(state.glob('*.json')),[])
    def test_resume_does_not_enable_saving_that_was_already_disabled(self):
        with tempfile.TemporaryDirectory() as root:
            root=pathlib.Path(root);server=root/'server';server.mkdir();state=root/'leases'
            (server/'server.properties').write_text('enable-rcon=true\nrcon.password=not-logged\n')
            args=types.SimpleNamespace(server_dir=server,state_dir=state,phase='prepare',mcrcon='/bin/true',command_timeout=1,simulate_save_failure=False)
            replies=[types.SimpleNamespace(returncode=0,stdout='Saving is already turned off'),types.SimpleNamespace(returncode=0,stdout='Saved the game')]
            with patch.dict(os.environ,{'BACKUP_JOB_ID':str(uuid.uuid4())}),patch.object(hook.subprocess,'run',side_effect=replies):hook.run(args)
            args.phase='resume'
            # Keep the recorded job identity, and verify no RCON request is made.
            lease=json.loads(next(state.glob('*.json')).read_text())
            with patch.dict(os.environ,{'BACKUP_JOB_ID':lease['job_id']}),patch.object(hook.subprocess,'run') as command:
                hook.run(args);command.assert_not_called()

class MinecraftController(unittest.TestCase):
    def test_disconnected_client_does_not_prevent_next_request(self):
        with tempfile.TemporaryDirectory() as root:
            args=types.SimpleNamespace(state_dir=pathlib.Path(root))
            servers={'s1':{'directory':root,'world':'world'}}
            payload=json.dumps({'server':'s1','phase':'resume','job_id':str(uuid.uuid4())}).encode()
            with patch.object(control.minecraft_hook,'run') as run:
                server,client=socket.socketpair()
                with server:
                    client.sendall(payload);client.close()
                    control.handle(server,args,servers)
                server,client=socket.socketpair()
                with server,client:
                    client.sendall(payload);client.shutdown(socket.SHUT_WR)
                    control.handle(server,args,servers)
                    self.assertTrue(json.loads(client.recv(4096))['ok'])
                self.assertEqual(run.call_count,2)

    def test_read_timeout_returns_error_without_stopping_controller(self):
        connection=unittest.mock.Mock()
        connection.recv.side_effect=TimeoutError('client timed out')
        control.handle(connection,types.SimpleNamespace(),{})
        reply=json.loads(connection.sendall.call_args.args[0])
        self.assertFalse(reply['ok'])

if __name__=='__main__':unittest.main()
