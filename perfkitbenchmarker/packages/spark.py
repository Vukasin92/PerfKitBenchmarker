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


"""Module containing Spark installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import vm_util


SPARK_TAR_URL = ('mirror.switch.ch/mirror/apache/dist/spark/'
                 'spark-1.4.1/spark-1.4.1-bin-hadoop2.6.tgz')
SPARK_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'spark-1.4.1-bin-hadoop2.6')


def _Install(vm):
  """Installs Spark on the VM."""
  vm.RemoteCommand('cd {0} && '
                   'wget {1} && '
                   'tar zxvf spark-1.4.1-bin-hadoop2.6.tgz'.format(
                       vm_util.VM_TMP_DIR, SPARK_TAR_URL))


def YumInstall(vm):
  """Installs Spark on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs Spark on the VM."""
  _Install(vm)
