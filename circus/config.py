import glob
import os
import signal
import warnings
from fnmatch import fnmatch
try:
    import resource
except ImportError:
    resource = None     # NOQA

import six

from circus import logger
from circus.py3compat import sort_by_field
from circus.util import (DEFAULT_ENDPOINT_DEALER, DEFAULT_ENDPOINT_SUB,
                         DEFAULT_ENDPOINT_MULTICAST, DEFAULT_ENDPOINT_STATS,
                         StrictConfigParser, replace_gnu_args, to_signum,
                         to_bool, papa)


def watcher_defaults():
    return {
        'name': '',
        'cmd': '',
        'args': '',
        'numprocesses': 1,
        'warmup_delay': 0,
        'executable': None,
        'working_dir': None,
        'shell': False,
        'uid': None,
        'gid': None,
        'send_hup': False,
        'stop_signal': signal.SIGTERM,
        'stop_children': False,
        'max_retry': 5,
        'graceful_timeout': 30,
        'rlimits': dict(),
        'stderr_stream': dict(),
        'stdout_stream': dict(),
        'priority': 0,
        'use_sockets': False,
        'singleton': False,
        'copy_env': False,
        'copy_path': False,
        'hooks': dict(),
        'respawn': True,
        'autostart': True,
        'use_papa': False}


class DefaultConfigParser(StrictConfigParser):

    def __init__(self, *args, **kw):
        StrictConfigParser.__init__(self, *args, **kw)
        self._env = dict(os.environ)

    def set_env(self, env):
        self._env = dict(env)

    def get(self, section, option, **kwargs):
        res = StrictConfigParser.get(self, section, option, **kwargs)
        return replace_gnu_args(res, env=self._env)

    def items(self, section, noreplace=False):
        items = StrictConfigParser.items(self, section)
        if noreplace:
            return items

        return [(key, replace_gnu_args(value, env=self._env))
                for key, value in items]

    @staticmethod
    def _dget(value, default=None, type=str):
        if value is None:
            return default

        if type is int:
            value = int(value)
        elif type is bool:
            value = to_bool(value)
        elif type is float:
            value = float(value)
        elif type is not str:
            raise NotImplementedError()

        return value

    def dget(self, section, option, default=None, type=str):
        if not self.has_option(section, option):
            return default

        return self._dget(self.get(section, option), default, type)


def rlimit_value(val):
    if resource is not None and (val is None or len(val) == 0):
        return resource.RLIM_INFINITY
    else:
        return int(val)


def read_config(config_path):
    cfg = DefaultConfigParser()
    with open(config_path) as f:
        if hasattr(cfg, 'read_file'):
            cfg.read_file(f)
        else:
            cfg.readfp(f)

    current_dir = os.path.dirname(config_path)

    # load included config files
    includes = []

    def _scan(filename, includes):
        if os.path.abspath(filename) != filename:
            filename = os.path.join(current_dir, filename)

        paths = glob.glob(filename)
        if paths == []:
            logger.warn('%r does not lead to any config. Make sure '
                        'include paths are relative to the main config '
                        'file' % filename)
        includes += paths

    for include_file in cfg.dget('circus', 'include', '').split():
        _scan(include_file, includes)

    for include_dir in cfg.dget('circus', 'include_dir', '').split():
        _scan(os.path.join(include_dir, '*.ini'), includes)

    logger.debug('Reading config files: %s' % includes)
    return cfg, [config_path] + cfg.read(includes)


def expand_vars(value, env):
    if isinstance(value, six.string_types):
        return replace_gnu_args(value, env=env)
    elif isinstance(value, dict):
        return {key: expand_vars(v, env) for key, v in six.iteritems(value)}
    elif isinstance(value, list):
        return [expand_vars(v, env) for v in value]
    else:
        return value


def get_config(config_file):
    if not os.path.exists(config_file):
        raise IOError("the configuration file %r does not exist\n" %
                      config_file)

    cfg, cfg_files_read = read_config(config_file)
    dget = cfg.dget
    config = {}

    # reading the global environ first
    global_env = dict(os.environ.items())
    local_env = dict()

    # update environments with [env] section
    if 'env' in cfg.sections():
        local_env.update(dict(cfg.items('env')))
        global_env.update(local_env)

    # always set the cfg environment
    cfg.set_env(global_env)

    # main circus options
    config['check_delay'] = dget('circus', 'check_delay', 5., float)
    config['endpoint'] = dget('circus', 'endpoint', DEFAULT_ENDPOINT_DEALER)
    config['endpoint_owner'] = dget('circus', 'endpoint_owner', None, str)
    config['pubsub_endpoint'] = dget('circus', 'pubsub_endpoint',
                                     DEFAULT_ENDPOINT_SUB)
    config['multicast_endpoint'] = dget('circus', 'multicast_endpoint',
                                        DEFAULT_ENDPOINT_MULTICAST)
    config['stats_endpoint'] = dget('circus', 'stats_endpoint', None)
    config['statsd'] = dget('circus', 'statsd', False, bool)
    config['umask'] = dget('circus', 'umask', None)
    if config['umask']:
        config['umask'] = int(config['umask'], 8)

    if config['stats_endpoint'] is None:
        config['stats_endpoint'] = DEFAULT_ENDPOINT_STATS
    elif not config['statsd']:
        warnings.warn("You defined a stats_endpoint without "
                      "setting up statsd to True.",
                      DeprecationWarning)
        config['statsd'] = True

    config['warmup_delay'] = dget('circus', 'warmup_delay', 0, int)
    config['httpd'] = dget('circus', 'httpd', False, bool)
    config['httpd_host'] = dget('circus', 'httpd_host', 'localhost', str)
    config['httpd_port'] = dget('circus', 'httpd_port', 8080, int)
    config['debug'] = dget('circus', 'debug', False, bool)
    config['debug_gc'] = dget('circus', 'debug_gc', False, bool)
    config['pidfile'] = dget('circus', 'pidfile')
    config['loglevel'] = dget('circus', 'loglevel')
    config['logoutput'] = dget('circus', 'logoutput')
    config['loggerconfig'] = dget('circus', 'loggerconfig', None)
    config['fqdn_prefix'] = dget('circus', 'fqdn_prefix', None, str)
    config['papa_endpoint'] = dget('circus', 'papa_endpoint', None, str)

    # Initialize watchers, plugins & sockets to manage
    watchers = []
    watchers_map = {}
    plugins = []
    sockets = []

    for section in cfg.sections():
        section_items = dict(cfg.items(section))
        if list(section_items.keys()) in [[], ['__name__']]:
            # Skip empty sections
            continue
        if section.startswith("socket:"):
            sock = section_items
            sock['name'] = section.split("socket:")[-1].lower()
            sock['so_reuseport'] = dget(section, "so_reuseport", False, bool)
            sock['replace'] = dget(section, "replace", False, bool)
            sockets.append(sock)

        if section.startswith("plugin:"):
            plugin = section_items
            plugin['name'] = section
            if 'priority' in plugin:
                plugin['priority'] = int(plugin['priority'])
            plugins.append(plugin)

        if section.startswith("watcher:"):
            watcher = watcher_defaults()
            watcher['name'] = section.split("watcher:", 1)[1]

            watcher['copy_env'] = dget(section, 'copy_env', False, bool)
            if watcher['copy_env']:
                watcher['env'] = dict(global_env)
            else:
                watcher['env'] = dict(local_env)

            watchers.append(watcher)
            watchers_map[section] = watcher

    # making sure we return consistent lists
    sort_by_field(watchers)
    sort_by_field(plugins)
    sort_by_field(sockets)

    # build environment for watcher sections
    for section in cfg.sections():
        if section.startswith('env:'):
            section_elements = section.split("env:", 1)[1]
            watcher_patterns = [s.strip() for s in section_elements.split(',')]
            env_items = dict(cfg.items(section, noreplace=True))

            for pattern in watcher_patterns:
                match = [w for w in watchers if fnmatch(w['name'], pattern)]

                for watcher in match:
                    watcher['env'].update(env_items)

    # Second pass to make sure env sections apply to all watchers.
    for section in cfg.sections():
        if section.startswith("watcher:"):
            watcher = watchers_map[section]

            env = dict(global_env)
            env.update(watcher['env'])

            # create watcher options
            for opt, val in cfg.items(section, noreplace=True):
                val = expand_vars(val, env)

                if opt in ('cmd', 'args', 'working_dir', 'uid', 'gid'):
                    watcher[opt] = val
                elif opt == 'numprocesses':
                    watcher['numprocesses'] = cfg._dget(val, 1, int)
                elif opt == 'warmup_delay':
                    watcher['warmup_delay'] = cfg._dget(val, 0, int)
                elif opt == 'executable':
                    watcher['executable'] = cfg._dget(val, None, str)
                # default bool to False
                elif opt in ('shell', 'send_hup', 'stop_children',
                             'close_child_stderr', 'use_sockets', 'singleton',
                             'copy_env', 'copy_path', 'close_child_stdout'):
                    watcher[opt] = cfg._dget(val, False, bool)
                elif opt == 'stop_signal':
                    watcher['stop_signal'] = to_signum(val)
                elif opt == 'max_retry':
                    watcher['max_retry'] = cfg._dget(val, 5, int)
                elif opt == 'graceful_timeout':
                    watcher['graceful_timeout'] = cfg._dget(val, 30, int)
                elif opt.startswith('stderr_stream') or \
                        opt.startswith('stdout_stream'):
                    stream_name, stream_opt = opt.split(".", 1)
                    watcher[stream_name][stream_opt] = val
                elif opt.startswith('rlimit_'):
                    limit = opt[7:]
                    watcher['rlimits'][limit] = rlimit_value(val)
                elif opt == 'priority':
                    watcher['priority'] = cfg._dget(val, 0, int)
                elif opt == 'use_papa' and cfg._dget(val, False, bool):
                    if papa:
                        watcher['use_papa'] = True
                    else:
                        warnings.warn("Config file says use_papa but the papa "
                                      "module is missing.",
                                      ImportWarning)
                elif opt.startswith('hooks.'):
                    hook_name = opt[len('hooks.'):]
                    val = [elmt.strip() for elmt in val.split(',', 1)]
                    if len(val) == 1:
                        val.append(False)
                    else:
                        val[1] = to_bool(val[1])

                    watcher['hooks'][hook_name] = val
                # default bool to True
                elif opt in ('check_flapping', 'respawn', 'autostart',
                             'close_child_stdin'):
                    watcher[opt] = cfg._dget(val, True, bool)
                else:
                    # freeform
                    watcher[opt] = val

    config['watchers'] = watchers
    config['plugins'] = plugins
    config['sockets'] = sockets
    return config
