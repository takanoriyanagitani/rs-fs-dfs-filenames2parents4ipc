#!/bin/bash

set -u

wsm="./target/wasm32-wasip1/release-wasi/dfs-filenames2parents4ipc.wasm"

find \
  ./src |
  wasmtime \
    run \
    "${wsm}" |
  arrow-cat
