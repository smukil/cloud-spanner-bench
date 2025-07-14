# Cloud Spanner Benchmark Java

### Load Data:

`./load_data.sh --project <your_cloudspanner_project> --instance <your_instance> --database <your_db> --numKeys 5000000`

### Run benchmarks:

**findOrCreateNode:**
`./run_bench.sh --project <your_cloudspanner_project> --instance <your_instance> --database <your_db> --mode findOrCreateNode --numThreads 386 --numTotalKeys 5000000 --numRuns 5000000 --keyStartOffset 0`

**findOrCreateEdge:**
`./run_bench.sh --project <your_cloudspanner_project> --instance <your_instance> --database <your_db> --mode findOrCreateEdge --numThreads 128 --numTotalKeys 5000000 --numRuns 5000000 --keyStartOffset 0`

**findSubGraph:**
`./run_bench.sh --project <your_cloudspanner_project> --instance <your_instance> --database <your_db> --mode findSubGraph --numThreads 64 --numTotalKeys 5000000 --numRuns 5000000 --keyStartOffset 0`

Notes:
1. Client machines used to test each have: 24vCPU | 96GB RAM
2. For findOrCreate*() calls: Use different `keyStartOffset` for each client if running multiple of them (at intervals of `numTotalKeys`)
3. Use lower client threads for more expensive calls

