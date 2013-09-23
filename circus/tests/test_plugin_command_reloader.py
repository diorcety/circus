from mock import patch

from circus.plugins.command_reloader import CommandReloader
from circus.tests.support import TestCircus
from circus.util import (DEFAULT_ENDPOINT_DEALER, DEFAULT_ENDPOINT_SUB)


class TestCommandReloader(TestCircus):

    def make_plugin(self, **config):
        config['active'] = True
        plugin = CommandReloader(DEFAULT_ENDPOINT_DEALER,
                                 DEFAULT_ENDPOINT_SUB, 1, None, **config)
        return plugin

    def setup_os_mock(self, realpath, mtime):
        patcher = patch('circus.plugins.command_reloader.os')
        os_mock = patcher.start()
        self.addCleanup(patcher.stop)
        os_mock.path.realpath.return_value = realpath
        os_mock.stat.return_value.st_mtime = mtime
        return os_mock

    def setup_call_mock(self, watcher_name):
        patcher = patch.object(CommandReloader, 'call')
        call_mock = patcher.start()
        self.addCleanup(patcher.stop)
        call_mock.side_effect = [
            {'watchers': [watcher_name]},
            {'options': {'cmd': watcher_name}},
            None,
        ]
        return call_mock

    def test_default_loop_rate(self):
        plugin = self.make_plugin()
        self.assertEqual(plugin.loop_rate, 1)

    def test_non_default_loop_rate(self):
        plugin = self.make_plugin(loop_rate='2')
        self.assertEqual(plugin.loop_rate, 2)

    def test_mtime_is_modified(self):
        plugin = self.make_plugin()
        plugin.cmd_files = {'foo': {'path': '/bar/baz', 'mtime': 1}}
        self.assertTrue(plugin.is_modified('foo', 2, '/bar/baz'))

    def test_path_is_modified(self):
        plugin = self.make_plugin()
        plugin.cmd_files = {'foo': {'path': '/bar/baz', 'mtime': 1}}
        self.assertTrue(plugin.is_modified('foo', 1, '/bar/quux'))

    def test_look_after_kown_watcher_triggers_restart(self):
        call_mock = self.setup_call_mock(watcher_name='foo')
        self.setup_os_mock(realpath='/bar/foo', mtime=42)
        plugin = self.make_plugin()
        plugin.cmd_files = {'foo': {'path': 'foo', 'mtime': 1}}

        plugin.look_after()

        self.assertEqual(plugin.cmd_files, {
            'foo': {'path': '/bar/foo', 'mtime': 42}
        })
        call_mock.assert_called_with('restart', name='foo')

    def test_look_after_new_watcher_does_not_restart(self):
        call_mock = self.setup_call_mock(watcher_name='foo')
        self.setup_os_mock(realpath='/bar/foo', mtime=42)
        plugin = self.make_plugin()
        plugin.cmd_files = {}

        plugin.look_after()

        self.assertEqual(plugin.cmd_files, {
            'foo': {'path': '/bar/foo', 'mtime': 42}
        })
        # No restart, so last call should be for the 'get' command
        call_mock.assert_called_with('get', name='foo', keys=['cmd'])

    def test_missing_watcher_gets_removed_from_plugin_dict(self):
        self.setup_call_mock(watcher_name='bar')
        self.setup_os_mock(realpath='/bar/foo', mtime=42)
        plugin = self.make_plugin()
        plugin.cmd_files = {'foo': {'path': 'foo', 'mtime': 1}}

        plugin.look_after()

        self.assertNotIn('foo', plugin.cmd_files)
