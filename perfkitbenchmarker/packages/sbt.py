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


"""Module containing curl installation and cleanup functions."""


def YumInstall(vm):
  """Installs the curl package on the VM."""
  vm.Install('curl')
  vm.RemoteCommand('curl https://bintray.com/sbt/rpm/rpm | '
                   'sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo')
  vm.InstallPackages('sbt')


def AptInstall(vm):
  """Installs the curl package on the VM."""
  vm.RemoteCommand('echo "deb https://dl.bintray.com/sbt/debian /" | '
                   'sudo tee -a /etc/apt/sources.list.d/sbt.list')
  vm.RemoteCommand('sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 '
                   '--recv 642AC823 && '
                   'sudo apt-get update')
  vm.InstallPackages('sbt')
