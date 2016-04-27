# OpenIFS SCM over tiles

This is a layer on top of `scmtiles` that runs the ECMWF OpenIFS single-column model over a rectangular region.

## Installation

Download and install `scmtiles` using the bootstrap script.
Then, clone this repository to a directory inside the root of the scmtiles project directory.
Setup your configuration file using `run.cfg.template` as a base.
It is recommended to create one configuration file per experiment rather than overwriting.

## Tips

You can use the `scmenv.sh` script provided by the `scmtiles` bootstrap script to discover the paths to Python and `mpiexec` for your project:

    echo "which python" | ./scmenv.sh
    echo "which mpiexec" | ./scmenv.sh

These are helpful when writing job submission scripts.
