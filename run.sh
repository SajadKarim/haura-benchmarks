#!/usr/bin/env bash

function run {
  local vdev_type="$1"
  local name="$2"
  local mode="$3"
  shift 3

  local out_path="results/$(date -I)_${vdev_type}/${name}_$(date +%s)"
  #local out_path="results/$(date -I)/${name}_$(date +%s)"
  mkdir -p "$out_path"

  pushd "$out_path"

#  echo "wiping ssd"
#  blkdiscard /dev/disk/by-id/nvme-CT500P5SSD8_20512BF90C84

#  sleep 10

  echo "running $mode with these settings:"
  env | grep BETREE__
  env > "env"
  /home/skarim/myrepo/haura/betree/haura-benchmarks/target/release/bectl config print-active > "config"
  /home/skarim/myrepo/haura/betree/haura-benchmarks/target/release/betree-perf "$mode" "$@"

  echo "merging results into $out_path/out.jsonl"
  /home/skarim/myrepo/haura/betree/haura-benchmarks/target/release/json-merge \
    --timestamp-key epoch_ms \
    ./betree-metrics.jsonl \
    ./proc.jsonl \
    ./sysinfo.jsonl \
    | /home/skarim/myrepo/haura/betree/haura-benchmarks/target/release/json-flatten > "out.jsonl"

  popd

  sleep 60
}

cargo build --release

export RUST_LOG=warn
export BETREE_CONFIG="$PWD/perf-config.json"

function tiered() {
	#export PMEM_NO_CLWB=1
	#export BETREE__CACHE_SIZE=$((1 * 1024 * 1024 * 1024))
	#export BETREE__STORAGE__TIERS="[ [ \"/vol1/datafile1\" ], [ \"/vol2/datafile1\" ] ]"
	#export BETREE__STORAGE__TIERS="[ [ \"/vol2/datafile1\" ], [ \"/vol1/datafile1\" ] ]"
	#export BETREE__STORAGE__TIERS="[ [ { path = \"/vol1/datafile1\", direct = false } ], [ { path = \"/vol2/datafile1\", direct = false } ] ]"
	#export BETREE__STORAGE__TIERS="[ [ \"/mnt/pmemfs0/datafile1\" ], [ \"/mnt/pmemfs1/datafile1\" ] ]"
	export BETREE__STORAGE__TIERS="[ [ { path = \"/mnt/pmemfs0/haura/datafile\", len = $((100 * 1024 * 1024 * 1024)) } ], [ { path = \"/mnt/pmemfs1/haura/datafile\", len = $((100 * 1024 * 1024 * 1024 )) } ] ]"
	#export BETREE__STORAGE__TIERS="[ [ { path = \"/mnt/pmemfs0/datafile1\", len = $((100 * 1024 * 1024 * 1024)) } ], [ { path = \"/mnt/pmemfs1/datafile1\", len = $((100 * 1024 * 1024 * 1024 )) } ] ]"
	#export BETREE__STORAGE__TIERS="[ [ { mem = $((100 * 1024 * 1024 * 1024)) } ], [ { mem = $((100 * 1024 * 1024 * 1024)) } ] ]"

	#local vdev_type="dram"
	local vdev_type="pmem"
	#local vdev_type="ssd"
	#local vdev_type="pmem_fs"

  (
    export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
    run "$vdev_type" tiered1_all0_alloc tiered1
  )

  (
    export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
    run "$vdev_type" tiered1_id_alloc tiered1
  )

  (
    export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
    run "$vdev_type" tiered1_all1_alloc tiered1
  )
}

function zip_cache() {
  local F="$PWD/data/linux-5.12.13.zip"
  local F_CD_START=1040032667
  for cache_mib in 32 64 128 256 512 1024 2048 4096 8192; do
    (
      export BETREE__CACHE_SIZE=$(($cache_mib * 1024 * 1024))
      run "default" "zip_cache_$cache_mb" zip 4 100 10 "$F" "$F_CD_START"
    )
  done
}

function zip_mt() {
  local F="$PWD/data/linux-5.12.13.zip"
  local F_CD_START=1040032667
  for cache_mib in 256 512 1024 2048; do
    echo "using $cache_mib MiB of cache"
    (
      export BETREE__CACHE_SIZE=$(($cache_mib * 1024 * 1024))

      local total=10000

      for num_workers in 1 2 3 4 5 6 7 8 9 10; do
        echo "running with $num_workers workers"
        local per_worker=$(($total / $num_workers))
        local per_run=$(($per_worker / 10))

        run "default" "zip_mt_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
      done
    )
  done
}

function zip_tiered() {
  local F="$PWD/data/linux-5.12.13.zip"
  local F_CD_START=1 #242415017 #1040032667
  # for cache_mib in 256 512 1024; do
  for cache_mib in 32 64; do
    echo "using $cache_mib MiB of cache"
    (
      export BETREE__CACHE_SIZE=$(($cache_mib * 1024 * 1024))

      local total=10000

      #local vdev_type="dram"
      local vdev_type="pmem"
      #local vdev_type="ssd"
      #local vdev_type="pmem_fs"

      for num_workers in 1 2 3 4 5 6 7 8 9 10; do
        echo "running with $num_workers workers"
        local per_worker=$(($total / $num_workers))
        local per_run=$(($per_worker / 10))

        (
          export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
	  export BETREE__STORAGE__TIERS="[ [ \"/vol1/datafile1\" ] ]"
          run "$vdev_type" "zip_tiered_all0_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
        )

        (
          export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
	  export BETREE__STORAGE__TIERS="[ [ \"/mnt/pmemfs0/datafile1\" ], [ \"/mnt/pmemfs1/datafile1\" ] ]"
          run "$vdev_type" "zip_tiered_id_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
        )

        (
          export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
	  export BETREE__STORAGE__TIERS="[ [ \"/mnt/pmemfs0/datafile1\" ], [ \"/mnt/pmemfs1/datafile1\" ] ]"
          run "$vdev_type" "zip_tiered_all1_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
        )

        (
          export BETREE__STORAGE__TIERS="[ [ { mem = $((4 * 1024 * 1024 * 1024)) } ] ]"
          export BETREE__ALLOC_STRATEGY="[[0], [0], [], []]"
          run "$vdev_type" "zip_tiered_mem_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
        )
      done
    )
  done
}



function ingest() {
  local F="$PWD/data/linux-5.12.13.zip"
  local DISK=/dev/disk/by-id/ata-WDC_WD30EFRX-68EUZN0_WD-WMC4N2195306

  (
    export BETREE__STORAGE__TIERS="[ [ { file = \"$DISK\" } ] ]"
    export BETREE__DEFAULT_STORAGE_CLASS=0

    (
      export BETREE__COMPRESSION="None"
      run "default" ingest_hdd_none ingest "$F"
    )

    for level in $(seq 1 16); do
      (
        export BETREE__COMPRESSION="{ Zstd = { level = $level } }"
        run "default" "ingest_hdd_zstd_$level" ingest "$F"
      )
    done
  )
}

function switchover() {
  run "default" switchover_tiny switchover 32 "$((32 * 1024 * 1024))"
  run "default" switchover_small switchover 8 "$((128 * 1024 * 1024))"
  run "default" switchover_medium switchover 4 "$((2 * 1024 * 1024 * 1024))"
  run "default" switchover_large switchover 4 "$((8 * 1024 * 1024 * 1024))"
}

#zip_tiered
tiered
#(
  # export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
  #export RUST_LOG=info
  #export BETREE__CACHE_SIZE=8589934592
  #run rewrite1 rewrite $((500 * 1024 * 1024)) 4
#)
