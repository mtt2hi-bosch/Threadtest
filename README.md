# Threadtest

`threadtest` is a portable pthread-based command-line tool for Linux and QNX that:

- detects the host OS at startup (`Linux`, `QNX 7.1`, `QNX 8`, or generic `QNX`)
- starts one functional thread
- waits on shared named events with a common timeout
- runs CPU-consuming work on trigger or timeout
- optionally triggers another shared named event after work
- prints small runtime statistics when it exits

The implementation uses POSIX shared memory plus process-shared `pthread_mutex_t` and `pthread_cond_t` objects so the same event name is visible across multiple processes.

## Build

```sh
make
```

Cross-compiling is supported through `CROSS_COMPILE`:

```sh
make CROSS_COMPILE=ntoaarch64-
```

## Usage

Run `threadtest help`, `threadtest --help`, or invoke it without parameters.

```text
threadtest [options]

Options:
  -h, --help                 Show this help text
  -e, --events <list>        Comma-separated events to wait on
  -s, --set <name>           Event to trigger after work completes
  -i, --trigger-start <name> Trigger one event once before the worker thread starts
  -t, --timeout <value>      Common wait timeout (supports us, ms, s, m; fractional values allowed)
  -w, --work <value>         Busy-work duration (supports us, ms, s, m)
  -R, --runtime <value>      Maximum runtime before stopping (supports us, ms, s, m)
  -c, --cpu <all|index>      Run worker on all CPUs or pin to one CPU index
  -p, --policy <name>        Scheduler policy: other, fifo, rr
  -r, --priority <value>     Scheduler priority for the worker thread
  -n, --name <text>          Instance label shown in logs and stats
  -v, --verbose              Print loop-level messages

Accepted option forms:
  -o value
  -o=value
  --long value
  --long=value
```

If a duration has no unit suffix, seconds are assumed.

## Examples

### Single instance, timeout-driven work

```sh
./threadtest --timeout=2.5ms --work=300us --runtime=3s --name=solo
```

### Two instances that bounce events between each other

Terminal 1:

```sh
./threadtest --name=A --events=ping --set=pong --timeout=5ms --work=250us --runtime=5s
```

Terminal 2:

```sh
./threadtest --name=B --events=pong --set=ping --trigger-start=ping --timeout=5ms --work=250us --runtime=5s
```

### Dedicated CPU and real-time policy

```sh
sudo ./threadtest --events=evt1 --set=evt2 --timeout=1ms --work=200us \
  --cpu=2 --policy=fifo --priority=20 --runtime=10s
```

Real-time policies usually require elevated privileges.

## Notes

- Event triggers are non-queued: the worker observes that an event changed, not a FIFO of individual trigger payloads.
- The shared event hub is intentionally process-global so multiple program instances can cooperate through the same event names.
- On Linux, CPU pinning uses `pthread_setaffinity_np()`.
- On QNX, CPU pinning uses `ThreadCtl(_NTO_TCTL_RUNMASK, ...)` from the worker thread itself.
