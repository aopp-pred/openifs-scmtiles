# OpenIFS SCM over tiles

This is a layer on top of `scmtiles` that runs the ECMWF OpenIFS single-column
model over a rectangular region.


## Typical deployment

Download the scmtiles bootstrap script:

    cd /path/to/my/storage
    curl -o bootstrap_scmtiles.sh https://raw.githubusercontent.com/aopp-pred/scmtiles/master/bootstrap_scmtiles.sh

Next run the bootstrap script, specifying a directory you'd like the scmtiles
environment to be installed into. This directory can be anything you like,
here I'm using `scmtiles_expdir/` as an example:

    bash bootstrap_scmtiles.sh scmtiles_expdir

Once scmtiles is installed you can enter the scmtiles directory and proceed to
clone the openifs-scmtiles repository:

    cd scmtiles_expdir/
    git clone https://github.com/aopp-pred/openifs-scmtiles.git cascade

I've chosen to clone the repository into a directory named `cg-cascade/` as the
experiment I'll be setting up is related to a project named cg-cascade.

I can now enter the `cg-cascade/` directory and set up the run. I'll make a copy
of the template run configuration file and adjust it to my own needs:

    cd cg-cascade/
    cp run.cfg.template run.exp01.cfg

It is recommended to create one configuration file per experiment rather than
overwriting a single configuration file.

### Tips

You can use the `scmenv.sh` script provided by the `scmtiles` bootstrap script
to discover the paths to Python and `mpiexec` for your project:

    echo "which python" | ./scmenv.sh
    echo "which mpiexec" | ./scmenv.sh

These are helpful when writing job submission scripts.
