#!/bin/bash
#
# Shared helpers for the destor PBS metadata-scan pipeline. Sourced by both
# create_metadata_scan.pbs (the scan job) and import_metadata_scan.pbs (the
# import job) so the two jobs derive the *same* per-target processing directory
# and the import can locate the scan job's output.
#
# The processing directory MUST live on shared GLADE scratch (visible from every
# compute node), since the scan and import run as separate jobs on potentially
# different nodes. $SCRATCH is /glade/derecho/scratch/$USER on Casper/Derecho;
# both fallbacks below keep the path deterministic even if the env var is unset.

: "${SCRATCH:=/glade/derecho/scratch/${USER}}"
: "${FS_SCAN_DESTOR_SCRATCH:=${SCRATCH}/tmp/fs_scans/destor}"

# destor_proc_dir <target_dir>
#   Echo the deterministic processing directory for a scan target. The name is
#   the target's basename plus a short hash of its absolute path, so repeat runs
#   of the same target reuse the same directory while equal basenames under
#   different parents stay distinct.
destor_proc_dir() {
    local target
    target="$(realpath "${1}")"
    local hash
    hash="$(echo -n "${target}" | sha1sum | cut -c1-12)"
    echo "${FS_SCAN_DESTOR_SCRATCH}/$(basename "${target}")_${hash}"
}
