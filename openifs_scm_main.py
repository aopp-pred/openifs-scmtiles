#!/usr/bin/env python
"""
Run the OpenIFS single column model over a grid of cells.

The program takes a single input which is a configuration file in INI
format. The program can be run using MPI for parallelism.

"""
# Copyright 2016 Andrew Dawson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from scmtiles.task import TileTask

from openifs_scm import SCMTileRunner


# Keyword arguments for the SCMTileRunner class:
_OPENIFS_RUN_CONFIG = {'delete_run_directories': True}


def main():
    """Program entry point, creates MPI tasks and runs them."""
    task = TileTask(SCMTileRunner, runner_kwargs=_OPENIFS_RUN_CONFIG,
                    decompose_mode='cells')
    task.initialize()
    task.run()
    status = task.finalize()
    return status


if __name__ == '__main__':
    sys.exit(main())
