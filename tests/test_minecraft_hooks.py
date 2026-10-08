import importlib.util
import json
import os
import pathlib
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

PATH=pathlib.Path(__file__).resolve().parents[1]/'scripts/minecraft-hook.py'
spec=importlib.util.spec_from_file_location('minecraft_hook',PATH)
hook=importlib.util.module_from_spec(spec);spec.loader.exec_module(hook)

class MinecraftHooks(unittest.TestCase):
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

if __name__=='__main__':unittest.main()
