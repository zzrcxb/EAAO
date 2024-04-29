#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include "common.h"

// Timestamp related
typedef uint64_t timestamp;

#ifdef USE_REALTIME
#define ts_now time_ns
#else
#define ts_now _rdtsc
#endif

__always_inline static u64 get_timespec_ns(struct timespec *ts) {
    return ts->tv_nsec + ts->tv_sec * 1000000000ull;
}

__always_inline static u64 time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return get_timespec_ns(&ts);
}

// returns true if the target ns is missed
__always_inline static bool busy_wait_till(timestamp *now_ts, timestamp target,
                                           timestamp bound) {
    while (*now_ts < target) {
        __asm__ __volatile__("rep; nop" ::: "memory");
        *now_ts = ts_now();
    }

    return (bound != 0) && (*now_ts - target) >= bound;
}

static u32 NUM_CHANNELS = 10;
static u64 TSC_FREQ = 0;

inline u64 n_rdseed(u32 cnt) {
    u64 mask = 0ull;
    for (u32 i = 0; i < cnt; i++) {
        __asm__ __volatile__("mov $1, %%rcx\n\t"
                             "shl $1, %0\n\t"
                             "or %0, %%rcx\n\t"
                             "rdseed %%r8\n\t"
                             "cmovae %%rcx, %0\n\t"
                             : "+r"(mask)::"rcx", "r8", "cc", "memory");
    }
    return mask;
}

void rdseed_fail_test(int cnt) {
    u64 t_start, t_end, status;
    _timer_warmup();

    t_start = _timer_start();
    status = n_rdseed(cnt);
    t_end = _timer_end();

    for (i32 i = cnt - 1; i >= 0; i--) {
        printf("%u\n", _TEST_BIT(status, i));
    }
    fprintf(stderr, "Latency: %lu; Num fails: %lu\n", t_end - t_start,
            _count_ones(status));
}

timestamp calc_start_ts(timestamp ts, timestamp interval, u32 channel) {
    timestamp slice = interval / NUM_CHANNELS;

#ifndef USE_REALTIME
    interval = 200000000; // about 1ms
#endif

    if (ts == 0) {
        ts = ((ts_now() / interval) + 2) * interval;
    } else if (ts % interval != 0) {
        ts = ((ts / interval) + 1) * interval;
    }

    return ts + slice * channel;
}

typedef struct {
    timestamp ts;
    u32 signal;
    bool desync;
} emit_rec;

#define SLEEP_MARGIN_US 1000
#define BUSY_WAIT_TILL(ts, now, target)                                        \
    while ((target) > (now)) {                                                 \
        clock_gettime(CLOCK_REALTIME, &(ts));                                  \
        (now) = get_timespec_ns(&(ts));                                        \
        if ((target) > (now)) {                                                \
            uint64_t diff_us = ((target) - (now)) / 1000;                      \
            if (diff_us > SLEEP_MARGIN_US) {                                   \
                usleep(diff_us - SLEEP_MARGIN_US);                             \
            }                                                                  \
        }                                                                      \
    }

void rdseed_emitter(timestamp start, timestamp interval, timestamp err,
                    u32 n_emits, u32 gadget_size, u32 channel,
                    volatile emit_rec *records) {
    bool need_alloc = !records;
    if (need_alloc) {
        records = calloc(n_emits, sizeof(emit_rec));
    }
    if (!records) return;

#ifndef USE_REALTIME
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    u64 now_ns = get_timespec_ns(&ts);
    BUSY_WAIT_TILL(ts, now_ns, start);
    start = 0;
    // u64 now_ns = time_ns(), now_tsc = _rdtsc();
    // if (start > now_ns) {
    //     start = (u64)((double)(start - now_ns) * (TSC_FREQ / 1000000000.0) + now_tsc);
    // }
#endif

    timestamp next = calc_start_ts(start, interval, channel), _start_ts = next;
    timestamp now = ts_now();
    for (u32 n = 0; n < n_emits; n++) {
        if (busy_wait_till(&now, next, err)) {
            records[n].ts = now;
            records[n].desync = true;
        } else {
            u64 status = n_rdseed(gadget_size);
            records[n].signal = _count_ones(status);
            records[n].ts = now;
            records[n].desync = false;
        }
        next += interval;
    }

    if (need_alloc) {
        u32 errs = 0, desyncs = 0;
        for (u32 n = 0; n < n_emits; n++) {
            printf("%u %lu %u\n", records[n].desync, records[n].ts,
                records[n].signal);
            errs += (records[n].signal == 0);
            desyncs += (records[n].ts == 0 || records[n].desync);
        }
        fprintf(stderr, "Desyncs: %u; Errors: %u; Start: %lu\n", desyncs, errs,
                _start_ts);
        free((void *)records);
    }
}

struct active_rec {
    u64 start_ns, end_ns;
};

#define NS_GAP 100000 // 100us

int rdseed_loop(u32 duration_ms, u32 gadget_size) {
    const u32 max_n_recs = 10000;
    struct active_rec *records = calloc(max_n_recs, sizeof(struct active_rec));
    size_t size = 0;
    if (!records) return errno;

    u64 start = time_ns(), end = start + duration_ms * 1000000ull;
    u64 last_ns = start, cur_ns = start;
    while (cur_ns < end) {
        n_rdseed(gadget_size);
        n_rdseed(gadget_size);
        n_rdseed(gadget_size);
        cur_ns = time_ns();
        if (cur_ns > last_ns && (cur_ns - last_ns) > NS_GAP &&
            size < max_n_recs) {
            records[size].start_ns = start;
            records[size].end_ns = last_ns;
            start = cur_ns;
            size += 1;
        }
        last_ns = cur_ns;
    }

    records[size].start_ns = start;
    records[size].end_ns = cur_ns;
    size += 1;

    for (size_t i = 0; i < size; i++) {
        printf("%lu %lu\n", records[i].start_ns, records[i].end_ns);
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) return -EINVAL;

    u32 gadget_size = 5;
    timestamp interval = 100000;
    timestamp err = 500;
    u32 channel = 0;
    const char *s;
    s = getenv("GADGET_SIZE");
    if (s) gadget_size = strtoul(s, NULL, 10);

    s = getenv("INTERVAL");
    if (s) interval = strtoull(s, NULL, 10);

    s = getenv("ERR_BOUND");
    if (s) err = strtoull(s, NULL, 10);

    s = getenv("BROADCAST_CHANNEL");
    if (s) channel = strtoul(s, NULL, 10);

    s = getenv("NUM_CHANNELS");
    if (s) NUM_CHANNELS = strtoul(s, NULL, 10);

    s = getenv("TSC_FREQ");
    if (s) TSC_FREQ = strtoull(s, NULL, 10);

    fprintf(stderr,
            "GADGET_SIZE: %u; INTERVAL: %lu; ERR_BOUND: %lu; CHANNEL: %u; "
            "NUM_CHANNELS: %u\n",
            gadget_size, interval, err, channel, NUM_CHANNELS);

    if (channel >= NUM_CHANNELS) {
        fprintf(stderr,
                "ERROR: Only has %u channels, but you selected channel %u\n",
                NUM_CHANNELS, channel);
        return -EINVAL;
    }

    if (strcmp(argv[1], "emit") == 0) {
        if (argc < 4) return -EINVAL;
        u32 n_emits = strtoul(argv[2], NULL, 10);
        u64 start_ts = strtoull(argv[3], NULL, 10);
        rdseed_emitter(start_ts, interval, err, n_emits, gadget_size, channel, NULL);
    } else if (strcmp(argv[1], "loop") == 0) {
        if (argc < 3) return -EINVAL;
        u64 duration_ms = strtoull(argv[2], NULL, 10);
        rdseed_loop(duration_ms, gadget_size);
    } else {
        fprintf(stderr, "Unknown option '%s'\n", argv[1]);
        return -EINVAL;
    }
    return 0;
}
