#!/usr/bin/env bash


set -e

path="/lustre/desc1/glade_p_archive"
path="$(realpath ${path})"

lfs find \
    ${path} \
    --lazy \
    --printf "%LF s=%s b=%b u=%U g=%G p=%LP type=%y perm=%m a=%A@ m=%T@ c=%C@ -- %p\n"
