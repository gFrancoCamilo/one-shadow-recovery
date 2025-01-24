from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
from json import load, dump
import subprocess

from benchmark.config_mal import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class BenchMal:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        self.num_of_twins = 0
        self.allow_communications_at_round = 0
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the Rocksdb installer (needed for Rocksdb).
            'sudo apt-get install -y clang',

            # Clone the repo.
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        

        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        i = 0
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename, i).split()
            i += 1
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings.consensus_port}' for x in hosts]
        front_addr = [f'{x}:{self.settings.front_port}' for x in hosts]
        mempool_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        #client_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        committee = Committee(names, consensus_addr, front_addr, mempool_addr, self.num_of_twins)
        committee.print(PathMaker.committee_file())
        print('hosts: ' + str(hosts))
        print('node_parameters: ' + str(node_parameters))
        print('self nodes: ' + str([x for x in self.node_parameters]))

        for i in range(len(hosts)):
            self.node_parameters[i].print(PathMaker.parameters_file(i))

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix='Uploading config files:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.put(PathMaker.committee_file(), '.')
            c.put(PathMaker.key_file(i), '.')
            c.put(PathMaker.parameters_file(i), '.')

        return committee

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=True):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        addresses = [f'{x}:{self.settings.front_port}' for x in hosts]
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        timeout = node_parameters[0].timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]
        for host, addr, log_file in zip(hosts, addresses, client_logs):
            cmd = CommandMaker.run_client(
                addr,
                bench_parameters.tx_size,
                rate_share,
                timeout,
                nodes=addresses
            )
            self._background_run(host, cmd, log_file)

        # Run the nodes.
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        parameters = [PathMaker.parameters_file(i) for i in range(len(hosts))]
        i = 0
        for host, key_file, db, log_file, parameter in zip(hosts, key_files, dbs, node_logs, parameters):
            cmd = CommandMaker.run_node(
                key_file,
                PathMaker.committee_file(),
                db,
                parameter,
                i,
                #PathMaker.parameters_file(),
                debug=debug
            )
            i += 1
            self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(2 * node_parameters[0].timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, hosts, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))
            c.get(
                PathMaker.client_log_file(i), local=PathMaker.client_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, network_parameters_filepath, dns_filepath, debug=True):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        hosts = self.manager.hosts()
        all_ips = []
        for host in hosts:
            all_ips += hosts[host]

        
        #dns = {}
        #for i in range(len(all_ips)):
        #    dns[i] = all_ips[i] + ":10000"
        #print(dns)
        #with open("./benchmark/.dns.json","w") as f:
        #    dump(dns, f, indent=4, sort_keys=True)
        with open(network_parameters_filepath) as f:
            data = load(f)
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            selected_hosts = self._select_hosts(bench_parameters)
            
            nodes = max(bench_parameters.nodes)
            test={}
            dns_no_port = {}
            region_one = list(hosts.keys())[0]
            i = 0
            for machine in hosts[region_one]:
                test[i] = machine + ':10000'
                dns_no_port[i] = machine
                i += 1
            honests = i
            malicious = (nodes - honests)//2
            for region in hosts:
                if region != region_one:
                    for j in range(0, len(hosts[region]), 2):
                        if j + 1 < len(hosts[region]):
                            test[honests] = hosts[region][j]+':10000'
                            test[honests + malicious] = hosts[region][j + 1]+':10000'
                            #test[honests+j*(len(hosts[region])-2)] = hosts[region][j]+':10000'
                            #dns_no_port[honests+j*(len(hosts[region])+1)] = hosts[region][j]
                            dns_no_port[honests] = hosts[region][j]
                            dns_no_port[honests + malicious] = hosts[region][j + 1]
                            honests += 1

            dns = test
            #dns = {}
            #for i in range(len(all_ips)):
            #    dns[i] = selected_hosts[i] + ":10000"
            print(dns)
            with open("./benchmark/.dns.json","w") as f:
                dump(dns, f, indent=4, sort_keys=True)

            with open(dns_filepath) as sf:
                dns = load(sf)

            self.node_parameters = []
            for i in range(bench_parameters_dict['nodes']):
                self.node_parameters.append(NodeParameters(node_parameters_dict, data['node_'+str(i)], data['allow_communications_at_round'], data['network_delay'], dns))
            #print('self nodes: ' + str([x for x in self.node_parameters]))
            #node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)
        
        # update the number of malicious nodes (that are not crashing)
        self.num_of_twins = data['num_of_twins']
        self.allow_communications_at_round = data['allow_communications_at_round']

        # Select which hosts to use.
        #selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        #dns_keys = list(dns.keys())
        print(dns_no_port)
        for i in range(len(dns)):
            selected_hosts[i] = dns_no_port[i]
        # Run benchmarks.
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')
                hosts = selected_hosts[:n]

                # Upload all configuration files.
                try:
                    self._config(hosts, node_parameters_dict)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError('Failed to configure nodes', e))
                    continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[:n-faults]

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, self.node_parameters, debug
                        )
                        self._logs(hosts, faults).print(PathMaker.result_file(
                            faults, n, r, bench_parameters.tx_size
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
