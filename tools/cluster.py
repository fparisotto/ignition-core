#!/usr/bin/env python

"""
Spark cluster management script.

This line is to make pylint happy

"""

import argh
from argh import ArghParser, CommandError
from argh.decorators import named, arg
import subprocess
from subprocess import check_output, check_call
from utils import tag_instances, get_masters, get_active_nodes
from utils import check_call_with_timeout
import os
import sys
from datetime import datetime
import time
import logging
import getpass
import json
import glob
import webbrowser
import ssl

log = logging.getLogger()
log.setLevel(logging.INFO)
#log.setLevel(logging.DEBUG)
formatter = logging.Formatter('cluster - %(asctime)s - %(levelname)s - %(message)s')
handler = log.handlers[0]
handler.setFormatter(formatter)
log.addHandler(handler)

script_path = os.path.dirname(os.path.realpath(__file__))

default_instance_type = 'r3.xlarge'
default_spot_price = '0.10'
default_worker_instances = '1'
default_executor_instances = '1'
default_master_instance_type = ''
default_driver_heap_size = '12G'
default_min_root_ebs_size_gb = '30'
default_region = 'us-east-1'
default_zone = default_region + 'b'
default_key_id = 'ignition_key'
default_key_file = os.path.expanduser('~/.ssh/ignition_key.pem')
default_ami = 'ami-611e7976'
default_master_ami = ''
default_env = 'dev'
default_spark_version = '2.0.2'
default_hdfs_version = '2.7.2'
default_spark_download_source = 'https://s3.amazonaws.com/chaordic-ignition-public/spark-{v}-bin-hadoop2.7.tgz'
default_hdfs_download_source = 'https://s3.amazonaws.com/chaordic-ignition-public/hadoop-{v}.tar.gz'
default_remote_user = 'ec2-user'
default_installation_user = 'root'
default_remote_control_dir = '/tmp/Ignition'
default_collect_results_dir = '/tmp'
default_user_data = os.path.join(script_path, 'scripts', 'noop')
default_defaults_filename = 'cluster_defaults.json'


master_post_create_commands = [
    ['sudo', 'yum', '-y', 'install', 'tmux'],
]


def get_aws_keys_str():
    return 'AWS_ACCESS_KEY_ID={0} AWS_SECRET_ACCESS_KEY={1}'.format(os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'))


def get_defaults(directory=None, defaults_filename=default_defaults_filename):
    directory = os.path.normpath(directory or get_module_path())

    defaults_file = os.path.join(directory, defaults_filename)
    if os.path.exists(defaults_file):
        with open(defaults_file) as f:
            # return the configuration as dictionary-like
            return json.load(f)

    parent_directory = os.path.normpath(os.path.join(directory, '..'))
    if parent_directory != directory:
        return get_defaults(directory=parent_directory, defaults_filename=defaults_filename)
    else:
        # we are stuck and no file found, return blank defaults
        return {}


def logged_call_base(func, args, tries):
    for i in range(tries - 1):
        log.debug('Calling with retry: %s', args)
        try:
            return func(args)
        except Exception as e:
            log.exception('Got exception, retrying...')
    log.debug('Calling: %s', args)
    return func(args)

# We suppose we are in a sub sub directory of the root (like: root-project/core/tools/cluster.py)
def get_module_path():
    return os.path.realpath(os.path.join(script_path, '..'))


def get_project_path():
    return os.path.realpath(os.path.join(get_module_path(), '..'))


def logged_call_output(args, tries=1):
    return logged_call_base(check_output, args, tries)


def logged_call(args, tries=1):
    return logged_call_base(check_call, args, tries)


def ssh_call(user, host, key_file, args=(), allocate_terminal=True, get_output=False):
    base = ['ssh', '-q']
    if allocate_terminal:
        base += ['-tt']
    base += ['-i', key_file,
             '-o', 'StrictHostKeyChecking=no',
             '{0}@{1}'.format(user, host)]
    base += args
    if get_output:
        return logged_call_output(base)
    else:
        return logged_call(base)

def ec2_script_base_path():
    return os.path.join(script_path, 'flintrock')

def chdir_to_ec2_script_and_get_path():
    ec2_script_base = ec2_script_base_path()
    os.chdir(ec2_script_base)
    ec2_script_path = os.path.join(ec2_script_base, 'standalone.py')
    return ec2_script_path


def call_ec2_script(args, timeout_total_minutes, timeout_inactivity_minutes, stdout=None):
    ec2_script_path = chdir_to_ec2_script_and_get_path()
    return check_call_with_timeout(['/usr/bin/env', 'python3', '-u',
                                    ec2_script_path] + args,
                                    stdout=stdout,
                                    timeout_total_minutes=timeout_total_minutes,
                                    timeout_inactivity_minutes=timeout_inactivity_minutes)


def cluster_exists(cluster_name, region):
    try:
        get_master(cluster_name, region=region)
        return True
    except Exception as e:
        return False


def parse_tags(tag_list):
    """
    >>> 'tag2' in parse_tags(['tag1=value1', 'tag2=value2'])
    True
    """
    tags = {}
    for t in tag_list:
        k, v = t.split('=')
        tags[k] = v
    return tags

def save_cluster_args(master, key_file, remote_user, all_args):
    ssh_call(user=remote_user, host=master, key_file=key_file,
             args=["echo '{}' > /tmp/cluster_args.json".format(json.dumps(all_args))])

def load_cluster_args(master, key_file, remote_user):
    return json.loads(ssh_call(user=remote_user, host=master, key_file=key_file,
                               args=["cat", "/tmp/cluster_args.json"], get_output=True))

# Util to be used by external scripts
def save_extra_data(data_str, cluster_name, region=default_region, key_file=default_key_file, remote_user=default_remote_user, master=None):
    master = master or get_master(cluster_name, region=region)
    ssh_call(user=remote_user, host=master, key_file=key_file,
             args=["echo '{}' > /tmp/cluster_extra_data.txt".format(data_str)])

def load_extra_data(cluster_name, region=default_region, key_file=default_key_file, remote_user=default_remote_user, master=None):
    master = master or get_master(cluster_name, region=region)
    return ssh_call(user=remote_user, host=master, key_file=key_file,
                    args=["cat", "/tmp/cluster_extra_data.txt"], get_output=True)



tag_help_text = 'Use multiple times, like: --tag tag1=value1 --tag tag2=value'


@argh.arg('-t', '--tag', action='append', type=str,
          help=tag_help_text)
@named('tag-instances')
def tag_cluster_instances(cluster_name, tag=[], env=default_env, region=default_region):
    tags = {'env': env, 'spark_cluster_name': cluster_name}
    tags.update(get_defaults().get('tags', {}))
    tags.update(parse_tags(tag))
    tag_instances(cluster_name, tags, region=region)


@argh.arg('-t', '--tag', action='append', type=str,
          help=tag_help_text)
def launch(cluster_name, slaves,
           key_file=default_key_file,
           env=default_env,
           tag=[],
           key_id=default_key_id, region=default_region,
           zone=default_zone, instance_type=default_instance_type,
           # TODO: implement it in flintrock
           ondemand=False,
           spot_price=default_spot_price,
           # TODO: implement it in flintrock
           master_spot=False,
           user_data=default_user_data,
           security_group=None,
           vpc=None,
           vpc_subnet=None,
           # TODO: consider implementing in flintrock
           master_instance_type=default_master_instance_type,
           executor_instances=default_executor_instances,
           min_root_ebs_size_gb=default_min_root_ebs_size_gb,
           retries_on_same_cluster=5,
           max_clusters_to_create=5,
           minimum_percentage_healthy_slaves=0.9,
           remote_user=default_remote_user,
           installation_user=default_installation_user,
           script_timeout_total_minutes=55,
           script_timeout_inactivity_minutes=10,
           just_ignore_existing=False,
           spark_download_source=default_spark_download_source,
           spark_version=default_spark_version,
           hdfs_download_source=default_hdfs_download_source,
           hdfs_version=default_hdfs_version,
           ami=default_ami,
           # TODO: consider implementing in flintrock
           master_ami=default_master_ami,
           instance_profile_name=None):

    assert not master_instance_type or master_instance_type == instance_type, 'Different master instance type is currently unsupported'
    assert not master_ami or master_ami == ami, 'Different master ami is currently unsupported'
    assert not ondemand, 'On demand is unsupported'
    assert master_spot, 'On demand master is currently unsupported'

    all_args = locals()

    if cluster_exists(cluster_name, region=region):
        if just_ignore_existing:
            log.info('Cluster exists but that is ok')
            return ''
        else:
            raise CommandError('Cluster already exists, pick another name')

    for j in range(max_clusters_to_create):
        log.info('Creating new cluster {0}, try {1}'.format(cluster_name, j+1))
        success = False

        auth_params = []

        # '--vpc-id', default_vpc,
        # '--subnet-id', default_vpc_subnet,
        if vpc and vpc_subnet:
            auth_params.extend([
                '--ec2-vpc-id', vpc,
                '--ec2-subnet-id', vpc_subnet,
            ])

        spot_params = ['--ec2-spot-price', spot_price] if not ondemand else []
        #master_spot_params = ['--master-spot'] if not ondemand and master_spot else []

        ami_params = ['--ec2-ami', ami] if ami else []
        #master_ami_params = ['--master-ami', master_ami] if master_ami else []

        iam_params = ['--ec2-instance-profile-name', instance_profile_name] if instance_profile_name else []

        for i in range(retries_on_same_cluster):
            log.info('Running script, try %d of %d', i + 1, retries_on_same_cluster)
            try:
                call_ec2_script(['--debug',
                                 'launch',
                                 '--ec2-identity-file', key_file,
                                 '--ec2-key-name', key_id,
                                 '--num-slaves', slaves,
                                 '--ec2-region', region,
                                 '--ec2-availability-zone', zone,
                                 '--ec2-instance-type', instance_type,
                                 '--ec2-min-root-ebs-size-gb', min_root_ebs_size_gb,
                                 '--assume-yes',
                                 '--install-spark',
                                 '--install-hdfs',
                                 '--spark-version', spark_version,
                                 '--hdfs-version', hdfs_version,
                                 '--spark-download-source', spark_download_source,
                                 '--hdfs-download-source', hdfs_download_source,
                                 '--spark-executor-instances', executor_instances,
                                 '--ec2-security-group', security_group,
                                 '--ec2-user', installation_user,
                                 '--ec2-user-data', user_data,
                                 cluster_name] +
                                spot_params +
                                auth_params +
                                ami_params +
                                iam_params,
                                timeout_total_minutes=script_timeout_total_minutes,
                                timeout_inactivity_minutes=script_timeout_inactivity_minutes)
                success = True
            except Exception as e:
                # Probably a timeout
                log.exception('Fatal error calling EC2 script')
                break
            finally:
                tag_cluster_instances(cluster_name=cluster_name, tag=tag, env=env, region=region)

            if success:
                break

        try:
            if success:
                master = get_master(cluster_name, region=region)
                save_cluster_args(master, key_file, remote_user, all_args)
                health_check(cluster_name=cluster_name, key_file=key_file, master=master, remote_user=remote_user, region=region)
                for command in master_post_create_commands:
                    ssh_call(user=remote_user, host=master, key_file=key_file, args=command)
                return master
        except Exception as e:
            log.exception('Got exception on last steps of cluster configuration')
        log.warn('Destroying unsuccessful cluster')
        destroy(cluster_name=cluster_name, region=region)
    raise CommandError('Failed to created cluster {} after failures'.format(cluster_name))


def destroy(cluster_name, delete_groups=False, region=default_region):
    assert not delete_groups, 'Delete groups is deprecated and unsupported'
    masters, slaves = get_active_nodes(cluster_name, region=region)

    all_instances = masters + slaves
    if all_instances:
        log.info('The following instances will be terminated:')
        for i in all_instances:
            log.info('-> %s' % i.public_dns_name)

        log.info('Terminating master...')
        for i in masters:
            i.terminate()
        log.info('Terminating slaves...')
        for i in slaves:
            i.terminate()
        log.info('Done.')


def get_master(cluster_name, region=default_region):
    masters = get_masters(cluster_name, region=region)
    if not masters:
        raise CommandError("No master on {}".format(cluster_name))
    return masters[0].public_dns_name


def ssh_master(cluster_name, key_file=default_key_file, user=default_remote_user, region=default_region, *args):
    master = get_master(cluster_name, region=region)
    ssh_call(user=user, host=master, key_file=key_file, args=args)


def rsync_call(user, host, key_file, args=[], src_local='', dest_local='', remote_path='', tries=3):
    rsync_args = ['rsync', '--timeout', '60', '-azvP']
    rsync_args += ['-e', 'ssh -i {} -o StrictHostKeyChecking=no'.format(key_file)]
    rsync_args += args
    rsync_args += [src_local] if src_local else []
    rsync_args += ['{0}@{1}:{2}'.format(user, host, remote_path)]
    rsync_args += [dest_local] if dest_local else []
    return logged_call(rsync_args, tries=tries)

def build_assembly():
    logged_call(['/bin/bash', '-c', '(cd {} && ./sbt assembly)'.format(get_project_path())])

def get_assembly_path():
    paths = glob.glob(get_project_path() + '/target/scala-*/*assembly*.jar')
    if paths:
        return paths[0]
    else:
        return None


@arg('job-mem', help='The amount of memory to use for this job (like: 80G)')
@arg('--master', help="This parameter overrides the master of cluster-name")
@arg('--disable-tmux', help='Do not use tmux. Warning: many features will not work without tmux. Use only if the tmux is missing on the master.')
@arg('--detached', help='Run job in background, requires tmux')
@arg('--destroy-cluster', help='Will destroy cluster after finishing the job')
@named('run')
def job_run(cluster_name, job_name, job_mem,
            key_file=default_key_file, disable_tmux=False,
            detached=False, notify_on_errors=False, yarn=False,
            job_user=getpass.getuser(),
            job_timeout_minutes=0,
            remote_user=default_remote_user, utc_job_date=None, job_tag=None,
            disable_wait_completion=False, collect_results_dir=default_collect_results_dir,
            remote_control_dir = default_remote_control_dir,
            remote_path=None, master=None,
            disable_assembly_build=False,
            kill_on_failure=False,
            destroy_cluster=False,
            region=default_region,
            driver_heap_size=default_driver_heap_size):

    utc_job_date_example = '2014-05-04T13:13:10Z'
    if utc_job_date and len(utc_job_date) != len(utc_job_date_example):
        raise CommandError('UTC Job Date should be given as in the following example: {}'.format(utc_job_date_example))
    disable_tmux = disable_tmux and not detached
    wait_completion = not disable_wait_completion or destroy_cluster
    master = master or get_master(cluster_name, region=region)

    project_path = get_project_path()
    project_name = os.path.basename(project_path)
    # Use job user on remote path to avoid too many conflicts for different local users
    remote_path = remote_path or '/home/%s/%s.%s' % (default_remote_user, job_user, project_name)
    remote_hook_local = '{module_path}/remote_hook.sh'.format(module_path=get_module_path())
    remote_hook = '{remote_path}/remote_hook.sh'.format(remote_path=remote_path)
    notify_param = 'yes' if notify_on_errors else 'no'
    yarn_param = 'yes' if yarn else 'no'
    job_date = utc_job_date or datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    job_tag = job_tag or job_date.replace(':', '_').replace('-', '_').replace('Z', 'UTC')
    tmux_wait_command = ';(echo Press enter to keep the session open && /bin/bash -c "read -t 5" && sleep 7d)' if not detached else ''
    tmux_arg = ". /etc/profile; . ~/.profile;tmux new-session {detached} -s spark.{job_name}.{job_tag} '{aws_vars} {remote_hook} {job_name} {job_date} {job_tag} {job_user} {remote_control_dir} {spark_mem} {yarn_param} {notify_param} {driver_heap_size} {tmux_wait_command}' >& /tmp/commandoutput".format(
        aws_vars=get_aws_keys_str(), job_name=job_name, job_date=job_date, job_tag=job_tag, job_user=job_user, remote_control_dir=remote_control_dir, remote_hook=remote_hook, spark_mem=job_mem, detached='-d' if detached else '', yarn_param=yarn_param, notify_param=notify_param, driver_heap_size=driver_heap_size, tmux_wait_command=tmux_wait_command)
    non_tmux_arg = ". /etc/profile; . ~/.profile;{aws_vars} {remote_hook} {job_name} {job_date} {job_tag} {job_user} {remote_control_dir} {spark_mem} {yarn_param} {notify_param} {driver_heap_size} >& /tmp/commandoutput".format(
        aws_vars=get_aws_keys_str(), job_name=job_name, job_date=job_date, job_tag=job_tag, job_user=job_user, remote_control_dir=remote_control_dir, remote_hook=remote_hook, spark_mem=job_mem, yarn_param=yarn_param, notify_param=notify_param, driver_heap_size=driver_heap_size)


    if not disable_assembly_build:
        build_assembly()

    assembly_path = get_assembly_path()
    if assembly_path is None:
        raise Exception('Something is wrong: no assembly found')

    ssh_call(user=remote_user, host=master, key_file=key_file,
             args=['mkdir', '-p', remote_path])

    rsync_call(user=remote_user,
               host=master,
               key_file=key_file,
               src_local=assembly_path,
               remote_path=with_leading_slash(remote_path))

    rsync_call(user=remote_user,
               host=master,
               key_file=key_file,
               src_local=remote_hook_local,
               remote_path=with_leading_slash(remote_path))

    if job_name == "zeppelin":
         webbrowser.open("http://{master}:8081".format(master=master))

    log.info('Will run job in remote host')
    if disable_tmux:
        ssh_call(user=remote_user, host=master, key_file=key_file, args=[non_tmux_arg], allocate_terminal=False)
    else:
        ssh_call(user=remote_user, host=master, key_file=key_file, args=[tmux_arg], allocate_terminal=True)

    if wait_completion:
        time.sleep(5) # wait job to set up before checking it
        failed = False
        failed_exception = None
        try:
            wait_for_job(cluster_name=cluster_name, job_name=job_name,
                         job_tag=job_tag, key_file=key_file, master=master,
                         region=region,
                         job_timeout_minutes=job_timeout_minutes,
                         remote_user=remote_user, remote_control_dir=remote_control_dir,
                         collect_results_dir=collect_results_dir)
        except JobFailure as e:
            failed = True
            failed_exception = e
            log.warn('Job failed with: {}'.format(e))
        except NotHealthyCluster as e:
            failed = True
            failed_exception = e
            log.warn('Job is running but cluster is unhealthy: {}'.format(e))
        except Exception as e:
            failed = True
            failed_exception = e
            log.exception('Unexpected exception while waiting for job')
        if failed and kill_on_failure:
            log.info('Trying to kill failed job...')
            try:
                kill_job(cluster_name=cluster_name, job_name=job_name,
                        job_tag=job_tag, key_file=key_file,
                        master=master, remote_user=remote_user,
                        region=region,
                        remote_control_dir=remote_control_dir)
                log.info('Killed!')
            except Exception as e:
                log.exception("Failed to kill failed job (probably it's already dead)")
        if destroy_cluster:
            log.info('Destroying cluster as requested')
            destroy(cluster_name, region=region)
        if failed:
            raise failed_exception or Exception('Failed!?')
    return (job_name, job_tag)


@named('attach')
def job_attach(cluster_name, key_file=default_key_file, job_name=None, job_tag=None,
               master=None, remote_user=default_remote_user, region=default_region):

    master = master or get_master(cluster_name, region=region)

    args = ['tmux', 'attach']
    if job_name and job_tag:
        args += ['-t', 'spark.{0}.{1}'.format(job_name, job_tag)]

    ssh_call(user=remote_user, host=master, key_file=key_file, args=args)

class NotHealthyCluster(Exception): pass

@named('health-check')
def health_check(cluster_name, key_file=default_key_file, master=None, remote_user=default_remote_user, region=default_region, retries=3):
    for i in range(retries):
        try:
            master = master or get_master(cluster_name, region=region)
            all_args = load_cluster_args(master, key_file, remote_user)
            nslaves = int(all_args['slaves'])
            minimum_percentage_healthy_slaves = all_args['minimum_percentage_healthy_slaves']
            masters, slaves = get_active_nodes(cluster_name, region=region)
            if nslaves == 0 or float(len(slaves)) / nslaves < minimum_percentage_healthy_slaves:
                raise NotHealthyCluster('Not enough healthy slaves: {0}/{1}'.format(len(slaves), nslaves))
            if not masters:
                raise NotHealthyCluster('No master found')
        except NotHealthyCluster, e:
            raise e
        except Exception, e:
            log.warning("Failed to check cluster health, cluster: %s, retries %s" % (cluster_name, i), exc_info=True)
            if i >= retries - 1:
                log.critical("Failed to check cluster health, cluster: %s, giveup!" % (cluster_name))
                raise e

class JobFailure(Exception): pass


def get_job_with_tag(job_name, job_tag):
    return '{job_name}.{job_tag}'.format(job_name=job_name, job_tag=job_tag)


def get_job_control_dir(remote_control_dir, job_with_tag):
    return '{remote_control_dir}/{job_with_tag}'.format(remote_control_dir=remote_control_dir, job_with_tag=job_with_tag)


def with_leading_slash(s):
    return s if s.endswith('/') else s + '/'

@named('collect-results')
def collect_job_results(cluster_name, job_name, job_tag,
                        key_file=default_key_file,
                        region=default_region,
                        master=None, remote_user=default_remote_user,
                        remote_control_dir=default_remote_control_dir,
                        collect_results_dir=default_collect_results_dir):
    master = master or get_master(cluster_name, region=region)

    job_with_tag = get_job_with_tag(job_name, job_tag)
    job_control_dir = get_job_control_dir(remote_control_dir, job_with_tag)

    rsync_call(user=remote_user,
               host=master,
               # Keep the RUNNING file so we can kill the job if needed
               args=['--remove-source-files', '--exclude', 'RUNNING'],
               key_file=key_file,
               dest_local=with_leading_slash(collect_results_dir),
               remote_path=job_control_dir)

    return os.path.join(collect_results_dir, os.path.basename(job_control_dir))


@named('wait-for')
def wait_for_job(cluster_name, job_name, job_tag, key_file=default_key_file,
                 master=None, remote_user=default_remote_user,
                 region=default_region,
                 remote_control_dir=default_remote_control_dir,
                 collect_results_dir=default_collect_results_dir,
                 job_timeout_minutes=0, max_failures=5, seconds_to_sleep=60):

    master = master or get_master(cluster_name, region=region)

    job_with_tag = get_job_with_tag(job_name, job_tag)

    log.info('Will wait remote status for job: {job_with_tag}'.format(job_with_tag=job_with_tag))

    job_control_dir = get_job_control_dir(remote_control_dir, job_with_tag)

    ssh_call_check_status = [
                '''([ ! -e {path} ] && echo WAITINGCONTROL) ||
                   ([ -e {path}/RUNNING ] && ps -p $(cat {path}/RUNNING) >& /dev/null && echo RUNNING) ||
                   ([ -e {path}/SUCCESS ] && echo SUCCESS) ||
                   ([ -e {path}/FAILURE ] && echo FAILURE) ||
                   echo KILLED'''.format(path=job_control_dir)
                ]

    def collect(show_tail):
        try:
            dest_log_dir = collect_job_results(cluster_name=cluster_name,
                                               job_name=job_name, job_tag=job_tag,
                                               key_file=key_file, region=region,
                                               master=master, remote_user=remote_user,
                                               remote_control_dir=remote_control_dir,
                                               collect_results_dir=collect_results_dir)
            log.info('Jobs results saved on: {}'.format(dest_log_dir))
            if show_tail:
                output_log = os.path.join(dest_log_dir, 'output.log')
                output_failure = os.path.join(dest_log_dir, 'FAILURE')
                try:
                    if os.path.exists(output_failure):
                        log.info('Tail of {}'.format(output_failure))
                        print(check_output(['tail', '-n', '40', output_failure]))
                    if os.path.exists(output_log):
                        log.info('Tail of {}'.format(output_log))
                        print(check_output(['tail', '-n', '40', output_log]))
                    else:
                        log.warn('Missing log file {}'.format(output_log))
                except Exception as e:
                    log.exception('Failed read log files')
            return dest_log_dir
        except Exception as e:
            log.exception('Failed to collect job results')

    failures = 0
    last_failure = None
    start_time = time.time()
    while True:
        try:
            output = (ssh_call(user=remote_user, host=master, key_file=key_file,
                               args=ssh_call_check_status, get_output=True) or '').strip()
            if output == 'SUCCESS':
                log.info('Job finished successfully!')
                collect(show_tail=False)
                break
            elif output == 'FAILURE':
                log.error('Job failed...')
                collect(show_tail=True)
                raise JobFailure('Job failed...')
            elif output == 'WAITINGCONTROL':
                log.warn('''Control directory is still missing. If this happens again on next check, perhaps the remote hook died before running''')
                commands = [
                    ['ls', '-lR', '/home', '/tmp'],
                    ['free', '-m'],
                    ['tmux', 'list-sessions'],
                    ['df', '-h'],
                    ['cat', '/tmp/commandoutput'],
                    ['ps', 'auxef']
                ]
                log.info('Will run some commands for posterior investigation of the problem')
                for command in commands:
                    ssh_call(user=remote_user, host=master, key_file=key_file, args=command)
                failures += 1
                last_failure = 'Control missing'
            elif output == 'KILLED':
                log.error('Job has been killed before finishing')
                collect(show_tail=True)
                raise JobFailure('Job has been killed before finishing')
            elif output == 'RUNNING':
                log.info('Job is running...')
            else:
                log.warn('Received unexpected response while checking job status: {}'.format(output))
                failures += 1
                last_failure = 'Unexpected response: {}'.format(output)
            health_check(cluster_name=cluster_name, key_file=key_file, master=master, remote_user=remote_user, region=region)
        except (subprocess.CalledProcessError, ssl.SSLError) as e:
            failures += 1
            log.exception('Got exception')
            last_failure = 'Exception: {}'.format(e)
        if failures > max_failures:
            log.error('Too many failures while checking job status, the last one was {}'.format(last_failure))
            collect(show_tail=True)
            raise JobFailure('Too many failures')
        if job_timeout_minutes > 0 and (time.time() - start_time) / 60 >= job_timeout_minutes:
            collect(show_tail=True)
            raise JobFailure('Timed out')
        log.debug('Sleeping for {} seconds before checking new status'.format(seconds_to_sleep))
        time.sleep(seconds_to_sleep)


@named('kill')
def kill_job(cluster_name, job_name, job_tag, key_file=default_key_file,
             master=None, remote_user=default_remote_user,
             region=default_region,
             remote_control_dir=default_remote_control_dir):

    master = master or get_master(cluster_name, region=region)

    job_with_tag = get_job_with_tag(job_name, job_tag)
    job_control_dir = get_job_control_dir(remote_control_dir, job_with_tag)

    ssh_call(user=remote_user, host=master, key_file=key_file,
        args=['''{
            pid=$(cat %s/RUNNING)
            children=$(pgrep -P $pid)
            sudo kill $pid $children
        } >& /dev/null''' % job_control_dir])


@named('killall')
def killall_jobs(cluster_name, key_file=default_key_file,
                 master=None, remote_user=default_remote_user,
                 region=default_region,
                 remote_control_dir=default_remote_control_dir):
    master = master or get_master(cluster_name, region=region)
    ssh_call(user=remote_user, host=master, key_file=key_file,
            args=[
            '''for i in {remote_control_dir}/*/RUNNING; do
                pid=$(cat $i)
                children=$(pgrep -P $pid)
                sudo kill $pid $children || true
            done >& /dev/null || true'''.format(remote_control_dir=remote_control_dir)
            ])

def check_flintrock_installation():
    try:
        with file('/dev/null', 'w') as devnull:
            call_ec2_script(['--help'], 1 , 1, stdout=devnull)
    except:
        setup = os.path.join(ec2_script_base_path(), 'setup.py')
        if not os.path.exists(setup):
            log.error('''
Flintrock is missing (or the wrong version is being used).
Check if you have checked out the submodule. Try:
  git submode update --init --recursive
Or checkout ignition with:
  git clone --recursive ....
''')
        else:
            log.error('''
Some dependencies are missing. For an Ubuntu system, try the following:
sudo apt-get install python3-yaml libyaml-dev python3-pip
sudo python3 -m pip install -U pip packaging setuptools
cd {flintrock}
sudo pip3 install -r requirements/user.pip
        '''.format(flintrock=ec2_script_base_path()))
        sys.exit(1)


parser = ArghParser()
parser.add_commands([launch, destroy, get_master, ssh_master, tag_cluster_instances, health_check])
parser.add_commands([job_run, job_attach, wait_for_job,
                     kill_job, killall_jobs, collect_job_results], namespace="jobs")

if __name__ == '__main__':
    check_flintrock_installation()
    parser.dispatch()
