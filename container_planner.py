"""
Flipkart Scrape Container Planner / Orchestrator

Determines:
  - Total Flipkart links in input JSON
  - Recommended number of Docker containers (one shard each) to finish within target time
  - Expected runtime for a given number of containers
  - Generates docker run commands (or dry-run JSON)

Assumes existing scraper image exposes CLI:
  python enhanced_flipkart_scraper_comprehensive.py --input-file /app/data/all_data.json --shard-index 0 --total-shards N --fast

Usage examples:
  python container_planner.py --input-file all_data.json --target-minutes 18 --assumed-seconds-per-link 3 --print-commands
  python container_planner.py --input-file all_data.json --containers 6 --assumed-seconds-per-link 2.8 --print-commands
  python container_planner.py --input-file all_data.json --target-minutes 15 --assumed-seconds-per-link 3 --json > plan.json

Mount strategy:
  - Bind current host directory to /app/data inside container.
  - Each container writes its shard output JSON.
  - After completion merge with:
    python enhanced_flipkart_scraper_comprehensive.py --input-file all_data.json --merge-shards --shard-prefix <prefix> --output-file merged.json
"""

import os
import json
import math
import argparse
import multiprocessing
from datetime import datetime

def count_flipkart_links(data) -> int:
    total = 0
    def walk(node):
        nonlocal total
        if isinstance(node, dict):
            sl = node.get("store_links")
            if isinstance(sl, list):
                for s in sl:
                    if isinstance(s, dict):
                        name = str(s.get("name","")).lower()
                        url = str(s.get("url","")).lower()
                        if "flipkart" in name or "flipkart.com" in url:
                            total += 1
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(data)
    return total

def plan(args):
    with open(args.input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_links = count_flipkart_links(data)
    if total_links == 0:
        raise SystemExit("No Flipkart links detected in input file.")

    target_seconds = args.target_minutes * 60 if args.target_minutes else None
    avg_sec = args.assumed_seconds_per_link

    cpu_cores = args.cpu_cores or multiprocessing.cpu_count()
    cpu_cap = int(math.ceil(cpu_cores * args.cpu_utilization_factor))
    mem_cap = None
    if args.total_memory_mb and args.memory_per_container_mb:
        usable_mem = max(0, args.total_memory_mb - args.memory_overhead_mb)
        mem_cap = usable_mem // args.memory_per_container_mb if args.memory_per_container_mb > 0 else None

    derived_needed = None
    if target_seconds:
        derived_needed = max(1, math.ceil((total_links * avg_sec) / target_seconds))

    # Final container count selection
    if args.containers:  # User forces container count
        containers = args.containers
    elif derived_needed:
        containers = derived_needed
    else:
        containers = 1

    # Apply caps
    if cpu_cap:
        containers = min(containers, cpu_cap)
    if mem_cap:
        containers = min(containers, mem_cap)
    if args.max_containers:
        containers = min(containers, args.max_containers)
    containers = max(1, containers)

    # Recompute expected runtime for final container count
    expected_runtime_seconds = (total_links * avg_sec) / containers
    expected_minutes = expected_runtime_seconds / 60

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    shard_prefix = args.shard_prefix or f"all_data_flipkart_{timestamp}"
    output_base = args.output_base or f"{os.path.splitext(os.path.basename(args.input_file))[0]}_flipkart_{timestamp}"

    commands = []
    remove_flag = "" if args.keep_containers else "--rm"

    for shard in range(containers):
        shard_out = f"/app/data/{output_base}.shard{shard+1}of{containers}.json"
        cname = f"fkshard_{shard+1}_{timestamp}"
        cmd = (
            f"docker run {remove_flag} --name {cname} "
            f"-v {os.path.abspath(args.host_mount)}:/app/data "
            f"{args.docker_image} "
            f"--input-file /app/data/{os.path.basename(args.input_file)} "
            f"--output-file {shard_out} "
            f"--shard-index {shard} --total-shards {containers} "
            f"--session-batch-size {args.session_batch_size} "
            f"{'--fast ' if args.fast else ''}"
        ).strip()
        commands.append({
            "shard": shard+1,
            "total_shards": containers,
            "container_name": cname,
            "output_file": shard_out,
            "command": cmd
        })

    plan_obj = {
        "input_file": args.input_file,
        "total_flipkart_links": total_links,
        "assumed_seconds_per_link": avg_sec,
        "target_minutes": args.target_minutes,
        "derived_needed_for_target": derived_needed,
        "cpu_cores": cpu_cores,
        "cpu_cap": cpu_cap,
        "memory_per_container_mb": args.memory_per_container_mb,
        "total_memory_mb": args.total_memory_mb,
        "memory_overhead_mb": args.memory_overhead_mb,
        "memory_cap": mem_cap,
        "requested_containers": args.containers,
        "final_containers": containers,
        "expected_runtime_minutes": round(expected_minutes, 2),
        "shard_prefix": shard_prefix,
        "output_base": output_base,
        "merge_instruction": (
            f"python enhanced_flipkart_scraper_comprehensive.py "
            f"--input-file {args.input_file} --merge-shards "
            f"--shard-prefix {output_base} --output-file {output_base}.merged.json"
        ),
        "commands": commands
    }

    if args.json:
        print(json.dumps(plan_obj, indent=2))
    else:
        print("=== Flipkart Scrape Container Plan ===")
        print(f"Input file:              {plan_obj['input_file']}")
        print(f"Total Flipkart links:    {plan_obj['total_flipkart_links']}")
        print(f"Avg seconds/link (assumed): {avg_sec}")
        if target_seconds:
            print(f"Target minutes:          {args.target_minutes}")
            print(f"Raw containers needed:   {derived_needed}")
        print(f"CPU cores:               {cpu_cores} (cap {cpu_cap})")
        if mem_cap is not None:
            print(f"Memory cap containers:   {mem_cap} (usable {args.total_memory_mb - args.memory_overhead_mb}MB)")
        if args.containers:
            print(f"User requested containers: {args.containers}")
        print(f"Final containers:        {containers}")
        print(f"Expected runtime (min):  {expected_minutes:.2f}")
        print(f"Output shard base:       {output_base}")
        print()
        if args.print_commands:
            print("Docker run commands:")
            for c in commands:
                print(f"  # Shard {c['shard']}/{containers}")
                print(f"  {c['command']}")
            print()
            print("Follow logs (example):")
            first = commands[0]['container_name']
            print(f"  docker logs -f {first}")
            print()
            print("Merge after completion:")
            print(f"  {plan_obj['merge_instruction']}")
        else:
            print("Use --print-commands to display docker run commands.")
    return plan_obj

def build_arg_parser():
    p = argparse.ArgumentParser(description="Plan and emit Docker container runs for Flipkart scraping shards.")
    p.add_argument('--input-file', default='all_data.json', help='Input JSON containing products (default all_data.json)')
    p.add_argument('--target-minutes', type=float, default=None, help='Desired completion time (minutes); if set, computes needed containers')
    p.add_argument('--assumed-seconds-per-link', type=float, default=3.0, help='Assumed average seconds per Flipkart link (default 3.0)')
    p.add_argument('--containers', type=int, default=None, help='Force a fixed number of containers (overrides target-based calculation)')
    p.add_argument('--max-containers', type=int, default=None, help='Hard upper bound after calculation')
    p.add_argument('--cpu-cores', type=int, default=None, help='Override detected CPU cores')
    p.add_argument('--cpu-utilization-factor', type=float, default=1.0, help='Multiplier of cores allowed as containers (default 1.0)')
    p.add_argument('--total-memory-mb', type=int, default=None, help='Total host memory MB (to cap by memory)')
    p.add_argument('--memory-per-container-mb', type=int, default=350, help='Estimated MB per container (default 350)')
    p.add_argument('--memory-overhead-mb', type=int, default=800, help='Reserved MB for OS/other processes (default 800)')
    p.add_argument('--session-batch-size', type=int, default=150, help='Links per Chrome session recycle (passed to scraper)')
    p.add_argument('--docker-image', default='flipkart-scraper:latest', help='Docker image to run')
    p.add_argument('--host-mount', default='.', help='Host directory to bind into /app/data (default current dir)')
    p.add_argument('--fast', action='store_true', help='Add --fast flag to scraper commands')
    p.add_argument('--output-base', default=None, help='Base name for shard output files (auto if omitted)')
    p.add_argument('--shard-prefix', default=None, help='(Legacy) optional explicit shard prefix (auto if omitted)')
    p.add_argument('--keep-containers', action='store_true', help='Do not auto-remove containers (omit --rm)')
    p.add_argument('--print-commands', action='store_true', help='Print docker run commands')
    p.add_argument('--json', action='store_true', help='Emit full plan as JSON')
    return p

if __name__ == '__main__':
    args = build_arg_parser().parse_args()
    plan(args)

# python enhanced_flipkart_scraper_comprehensive.py --input-file all_data.json --run-all-shards --workers 15 --fast --session-batch-size 150
# python container_planner.py --input-file all_data.json --containers 6 --assumed-seconds-per-link 3 --print-commands --fast | Out-String