#!/bin/bash
#BSUB -q par-multi
#BSUB -n 64
#BSUB -W 03:00

#-----------------------------------------------------------------------
# Write a message on stderr
#-----------------------------------------------------------------------
err () {
    echo "$@" 1>&2
}

#-----------------------------------------------------------------------
# Calculate a model start time from a job array index
#-----------------------------------------------------------------------
calc_start_time () {
    local array_index="$1"
    local base_time=$(date -u -d "2009-04-06 01:00:00")
    local offset_minutes=$(( (${array_index} - 1) * 15 ))
    date -u -d "${base_time}+${offset_minutes} minutes" -u +'%FT%T'
}

# The variable LSB_JOBINDEX must be set for this program to work, exit
# with an error if not.
if [[ -z "$LSB_JOBINDEX" ]]; then
    err "error: this job *must* be run as a job array"
    err "submit the job with:"
    err '    bsub -J "beta[1-10]"'
    err "substituting "1-10" with the job indices you require)"
    exit 1
fi

# Define the base directory for SCM deployment, this should be the
# directory this file is in:
readonly RUN_DIR="PATH/TO/EXPERIMENT/HERE"

# Full paths to the required executables:
#   - python (>= 3.5)
#   - mpiexec (version provided with mpi4py)
# 
# If you used the bootstrap_scmtiles.py script to set-up your experiment you
# can use:
#
#     echo "which python" | ./scmenv.sh
#
# and
#
#     echo "which mpiexec" | ./scmenv.sh
#
# to find the correct values for these paths.
readonly PYTHON="YOUR/PATH/HERE/python"
readonly MPIEXEC="YOUR/PATH/HERE/mpiexec"

# Change into the run directory and run the main program using MPI:
cd "$RUN_DIR"

# Construct the input file for the job:
start_time=$(calc_start_time $LSB_JOBINDEX)
run_cfg="configs/run.${start_time}.cfg"
sed -e "s/_STARTTIME_/${start_time}/" template.cfg > "$run_cfg"

# Run the job:
log_name="logs/run.${start_time}.log"
$MPIEXEC $PYTHON openifs_scm_main.py "$run_cfg" > "$log_name"
