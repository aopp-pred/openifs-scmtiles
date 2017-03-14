#!/bin/bash
#
# Launch scmtiles runs with post-processing jobs attached.
#

#=======================================================================
# Program constants
#=======================================================================

# Maximum simultaneous jobs in a job array:
readonly MAX_RUN_JOBS=5
readonly MAX_PP_JOBS=5

# Exit codes:
readonly E_BADARG=1
readonly E_BADJOB=3

# Program name:
readonly PROG=$(basename "$0")

# Program help:
readonly HELP="
Name:

    ${PROG} - submit scmtiles jobs with attached post-processing

Synopsis:

    ${PROG} [-h] name array_spec

Description:

    Submit an array job to run scmtiles, and a dependent array of
    post-processing tasks.

Options:

    -h
        Display this help message.

Examples:

    Run a job with base name \"myrun\" that runs elements 1-10 of the
    array:

        ${PROG} myrun 1-10

    Run a job with base name \"other_run\" that runs elements 13, 15, and
    18-21 of the array:

        ${PROG} other_run 13,15,18-21

Author:

    Andrew Dawson <andrew.dawson@physics.ox.ac.uk>
"

#=======================================================================
# Function definitions
#=======================================================================

#-----------------------------------------------------------------------
# Write a message to stderr
#-----------------------------------------------------------------------
err () {
  echo "$@" 1>&2
}

#-----------------------------------------------------------------------
# Launch a pair of job arrays for running and post-processing
#-----------------------------------------------------------------------
launch () {
  local jobname="$1"
  local array_expr="$2"
  local run_jobname="${jobname}_run"
  local pp_jobname="${jobname}_pp"
  # Check the run and pp job scripts exist:
  if ! [[ -f "run.sh" ]] || ! [[ -f "pp.sh" ]]; then
    err 'error: job scripts "run.sh" and "pp.sh" must exist'
    return $E_BADJOB
  fi
  # Launch the run job array:
  local run_log="logs/${run_jobname}.%I.log"
  bsub -J "${run_jobname}[${array_expr}]%${MAX_RUN_JOBS}" \
       -o "${run_log}" -e "${run_log}" < run.sh
  if [[ $? -ne 0 ]]; then
    err "error: submission of run.sh failed, check it is a valid LSF job"
    return $E_BADJOB
  fi
  # Launch the post-processing job array:
  local pp_log="logs/${pp_jobname}.%I.log"
  bsub -w "done(${run_jobname}[*])" \
       -J "${pp_jobname}[${array_expr}]%${MAX_PP_JOBS}" \
       -ti -o "${pp_log}" -e "${pp_log}" < pp.sh
  if [[ $? -ne 0 ]]; then
    err "error: submission of pp.sh failed, check it is a valid LSF job"
    return $E_BADJOB
  fi
  return 0
}

#=======================================================================
# Main program
#=======================================================================

main () {
  # Parse any command line options:
  while getopts ":h" opt; do
    case $opt in
      h)
        echo "$HELP"
        return 0
        ;;
      \?)
        err "error: invalid option: -${OPTARG}"
        err "  use -h for help"
        return $E_BADARG
        ;;
    esac
  done
  # Count and verify the positional arguments, we are expecting a job name
  # and an array specifiction:
  if [[ $# -ne 2 ]]; then
    err "error: 2 arguments are required"
    err "  use -h for help"
    return $E_BADARG
  fi
  local name="$1"
  local array_expr="$2"
  launch "$name" "$array_expr"
}

main $@
