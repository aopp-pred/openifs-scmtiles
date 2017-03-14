#!/bin/bash
#BSUB -q short-serial
#BSUB -n 1
#BSUB -W 12:00

# Define the directory where the temporary work was done, for cg-cascade
# this is probably "/work/scratch/cg-cascade/cgXX":
readonly WORK_DIR="PATH/TO/WORK/DIR/HERE"

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

start_time=$(calc_start_time $LSB_JOBINDEX)
job_dir="$WORK_DIR/$start_time"
for file_path in $job_dir/*y????x????; do
    rm -rf "$file_path"
done

