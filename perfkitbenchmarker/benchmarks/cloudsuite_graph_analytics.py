# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs CloudSuite Web Search benchmark.
Docs:
    http://parsa.epfl.ch/cloudsuite/
    Runs CloudSuite Graph Analytics to collect the statistics that show
    the running times of either PageRank, Connected Components
    or Triangle Count algorithms.
    Args:
        -app: Algorithm to run, for PageRank, Connected Components
        or Triangle Count, specify "pagerank", "cc", or "tc", respectively.
    """

import posixpath
import re

from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker.packages import spark

FLAGS = flags.FLAGS

flags.DEFINE_string('app', 'pagerank',
                    'algorithm that graph analytics benchmark will run')

BENCHMARK_INFO = {'name': 'cloudsuite_graph_analytics',
                  'description': 'Run CloudSuite Graph Analytics',
                  'scratch_disk': True,
                  'num_machines': 4}

CLOUDSUITE_GA_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'cloudsuite-ga')
TWITTER_DATASET_URL = ('socialcomputing.asu.edu/uploads/1296759055/'
                       'Twitter-dataset.zip')
GA_TAR_URL = ('curl --header "Host: doc-08-b0-docs.googleusercontent.com"'
              ' --header "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:'
              '38.0) Gecko/20100101 Firefox/38.0" --header "Accept: text/'
              'html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
              '" --header "Accept-Language: en-US,en;q=0.5" --header "Coo'
              'kie: AUTH_npu93f5qtsn6732s1bkk8mg68ja3runk_nonce=m1kb4h4r6'
              'a2g4" --header "Connection: keep-alive" "https://doc-08-b0'
              '-docs.googleusercontent.com/docs/securesc/ff1mikmg3qur6cba'
              'j9vilst0ftkfegee/madgr7fh54ucluj6fuhbe9167k4uupje/14410944'
              '00000/06154759484088948450/06154759484088948450/0BzrWXFS43'
              'eWAV3lna3M4Y2xaN1E?e=download&nonce=m1kb4h4r6a2g4&user=061'
              '54759484088948450&hash=vakv2nlnom7kj0s486is4njmk2hft89v" -'
              'o "graph_analytics.tar.gz" -L')
SPARK_MASTER_PORT = 7077
SPARK_MASTER_WEBUI_PORT = 8080
SPARK_UI_PORT = 4040
SPARK_WORKER_PORT = 7078
SPARK_WORKER_WEBUI_PORT = 8081
SPARK_BLOCK_MANAGER_PORT = 53192
SPARK_BROADCAST_PORT = 58408
SPARK_DRIVER_PORT = 38097
SPARK_EXECUTOR_PORT = 37065
SPARK_FILESERVER_PORT = 35328
SPARK_REPL_CLASS_SERVER_PORT = 37066


def GetInfo():
      return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Cloudsuite Graph Analytics and start the server on all machines.
    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
         required to run the benchmark.
  """
  vms = benchmark_spec.vms
  fw = benchmark_spec.firewall
  master = vms[0]
  for vm in vms:
    fw.AllowPort(vm, SPARK_BLOCK_MANAGER_PORT)
    fw.AllowPort(vm, SPARK_BROADCAST_PORT)
    fw.AllowPort(vm, SPARK_DRIVER_PORT)
    fw.AllowPort(vm, SPARK_EXECUTOR_PORT)
    fw.AllowPort(vm, SPARK_FILESERVER_PORT)
    fw.AllowPort(vm, SPARK_REPL_CLASS_SERVER_PORT)
    if vm == master:
      fw.AllowPort(vm, SPARK_MASTER_PORT)
      fw.AllowPort(vm, SPARK_MASTER_WEBUI_PORT)
      fw.AllowPort(vm, SPARK_UI_PORT)
    else:
      fw.AllowPort(vm, SPARK_WORKER_PORT)
      fw.AllowPort(vm, SPARK_WORKER_WEBUI_PORT)
    vm.Install('wget')
    vm.RemoteCommand('mkdir -p {0}'.format(
                     CLOUDSUITE_GA_DIR))
    vm.Install('spark')
    vm.RemoteCommand('cd {0}/conf && '
                     'cp spark-env.sh.template spark-env.sh && '
                     'cp spark-defaults.conf.template '
                     'spark-defaults.conf'.format(
                         spark.SPARK_HOME_DIR))
    spark_local_dir = posixpath.join(vm.GetScratchDir(), 'spark_local')
    vm.RemoteCommand('cd {0}/conf && '
                     'echo "{1}" >> spark-env.sh'.format(
                         spark.SPARK_HOME_DIR,
                         'export SPARK_LOCAL_DIRS=' + spark_local_dir))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-env.sh'.format(
                     'export SPARK_WORKER_PORT=' + repr(SPARK_WORKER_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_BLOCK_MANAGER_PORT ' + repr(
                         SPARK_BLOCK_MANAGER_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_BROADCAST_PORT ' + repr(SPARK_BROADCAST_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_DRIVER_PORT ' + repr(SPARK_DRIVER_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_EXECUTOR_PORT ' + repr(SPARK_EXECUTOR_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_FILESERVER_PORT ' + repr(SPARK_FILESERVER_PORT),
                     spark.SPARK_HOME_DIR))
    vm.RemoteCommand('echo "{0}" >> {1}/conf/spark-defaults.conf'.format(
                     'SPARK_REPL_CLASS_SERVER_PORT ' + repr(
                         SPARK_REPL_CLASS_SERVER_PORT),
                     spark.SPARK_HOME_DIR))
  master.Install('sbt')
  master.Install('curl')
  scratch_dir = master.GetScratchDir()
  edges_file = posixpath.join(scratch_dir, 'Twitter-dataset/data/edges.csv')
  master.RemoteCommand('cd {0} && '
                       '{1} && '
                       'tar zxf graph_analytics.tar.gz'.format(
                           CLOUDSUITE_GA_DIR, GA_TAR_URL))  # TODO: add wget {1}
  master.RemoteCommand('cd {0}/graph_analytics && '
                       'sed -i "s/EDGES_FILE/{1}/g" '
                       'src/main/scala/GraphAnalytics.scala && '
                       'sbt package'.format(
                           CLOUDSUITE_GA_DIR, re.escape(edges_file)))
  master.RemoteCommand('cd {0} && '
                       'wget {1} && '
                       'unzip Twitter-dataset.zip && '
                       'cd Twitter-dataset/data && '
                       'sed -i "s/,/ /g" edges.csv'.format(
                           scratch_dir, TWITTER_DATASET_URL))
  master.RemoteCommand('cd {0} && '
                       'echo {1} > conf/slaves && '
                       'echo {2} >> conf/slaves && '
                       'echo {3} >> conf/slaves && '
                       'sbin/start-all.sh'.format(
                           spark.SPARK_HOME_DIR, vms[1].ip_address,
                           vms[2].ip_address, vms[3].ip_address))


def Run(benchmark_spec):
  """Run Cloudsuite Graph Analytics on the target vm.
    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    Returns:
      A list of sample.Sample objects.
  """
  master = benchmark_spec.vms[0]
  jar_path = posixpath.join(CLOUDSUITE_GA_DIR,
                            'graph_analytics/target/scala-2.10/'
                            'graph-analytics_2.10-1.0.jar')
  stdout, _ = master.RemoteCommand('curl http://localhost:8080')
  url = re.findall(r'.*URL:\</strong> (.*)\</li>', stdout)[0]
  print url
  master.RobustRemoteCommand('cd {0} && '
                             'bin/spark-submit --class "GraphAnalytics" '
                             '--master {1} --driver-memory 8G '
                             '--executor-memory 8G {2} -app="{3}"'.format(
                                 spark.SPARK_HOME_DIR, url,
                                 jar_path, FLAGS.app))

  def ParseOutput(client):
    pass

  results = []
  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Graph Analytics on the target vm (by uninstalling).
  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = vms[0]
  master.RemoteCommand('cd {0} && '
                       'sbin/stop-all.sh'.format(
                           spark.SPARK_HOME_DIR))
  for vm in vms:
    scratch_dir = vm.GetScratchDir()
    if vm == master:
      vm.RemoteCommand('cd {0} && '
                       'rm Twitter-dataset.zip && '
                       'rm -rf Twitter-dataset'.format(
                           scratch_dir))
    vm.RemoteCommand('rm -r {0}/spark_local'.format(
                     scratch_dir))
