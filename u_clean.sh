#!/bin/bash
#
# Launch scmtiles manual cleanup.
#

#=======================================================================
# Program constants
#=======================================================================

# Maximum simultaneous jobs in an array:
readonly MAX_ARRAY_JOBS=5

# Exit codes:
readonly E_BADARG=1
readonly E_BADJOB=3

# Program name:
readonly PROG=$(basename "$0")

# Program help:
readonly HELP="
Name:

    ${PROG} - submit scmtiles manual cleanup jobs

Synopsis:

    ${PROG} [-h] name array_spec

Description:

    Submit an array job to run manual cleanup tasks.

Options:

    -h
        Display this help message.

Examples:

    Run a job with base name \"myrun\" that runs elements 1-10 of the
    array:

        ${PROG} myrun 1-10

    Run a job with base name \"other_run\" that runs elements 13, 15,
    and 18-21 of the array:

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
  local cu_jobname="${jobname}_cu"
  if ! [[ -f "clean.sh" ]]; then
    err 'error: job script "clean.sh" must exist'
    return $E_BADJOB
  fi
  # Launch the post-processing job array:
  local cu_log="logs/${cu_jobname}.%I.log"
  bsub -J "${cu_jobname}[${array_expr}]%${MAX_ARRAY_JOBS}" \
       -o "${cu_log}" -e "${cu_log}" < clean.sh
  if [[ $? -ne 0 ]]; then
    err "error: submission of clean.sh failed, check it is a valid LSF job"
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
  # and a job array specification:
  if [[ $# -ne 2 ]]; then
    err "error: 2 arguments are required"
    err "  use -h for help"
    return $E_BADARG
  fi
  local name="$1"
  local array_expr="$2"
  # Run the launcher routine and exit with its status:
  launch "$name" "$array_expr"
}

main $@
