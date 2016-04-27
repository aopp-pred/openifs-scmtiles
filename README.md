# OpenIFS SCM over tiles

This is a layer on top of `scmtiles` that runs the ECMWF OpenIFS single-column
model over a rectangular region.


## Typical deployment

Download the scmtiles bootstrap script, you'll need to know your Gitlab
private token (found in settings under account), and you'll probably want to
turn off shell history before typing it into your shell for security reasons.
In bash this can be done with `set +o history` (in tcsh try
`set HISTFILE=/dev/null`):

    cd /path/to/my/storage
    set +o history
    curl -o bootstrap_scmtiles.py https://gitlab.physics.ox.ac.uk/dawson/scmtiles/raw/master/bootstrap_scmtiles.py?private_token=your-private-token

Next run the bootstrap script, specifying a directory you'd like the scmtiles
environment to be installed into. This directory can be anything you like,
here I'm using `scmtiles_expdir/` as an example:

    python bootstrap_scmtiles.py scmtiles_expdir

Once scmtiles is installed you can enter the scmtiles directory and proceed to
clone the openifs-scmtiles repository:

    cd scmtiles_expdir/
    git clone git@gitlab.physics.ox.ac.uk:dawson/openifs-scmtiles.git cascade

I've chosen to clone the repository into a directory named `cascade/` as the
experiment I'll be setting up is related to CASCADE project data.

I can now enter the `cascade/` directory and set up the run. I'll make a copy
of the template run configuration file and adjust it to my own needs:

    cd cascade/
    cp run.cfg.template run.exp01.cfg

It is recommended to create one configuration file per experiment rather than
overwriting a single configuration file.

### Tips

You can use the `scmenv.sh` script provided by the `scmtiles` bootstrap script
to discover the paths to Python and `mpiexec` for your project:

    echo "which python" | ./scmenv.sh
    echo "which mpiexec" | ./scmenv.sh

These are helpful when writing job submission scripts.
