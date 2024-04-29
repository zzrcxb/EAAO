#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "common.h"

// A static threshold for how long gettime can run in TSC cycles
static u64 GETTIME_THRESH = 400;

#define NS_SCALE 1000000000ull
inline u64 get_timespec_ns(struct timespec *ts) {
    return ts->tv_nsec + ts->tv_sec * NS_SCALE;
}

inline u64 get_timespec_diff_nsec(struct timespec *tstart, struct timespec *tend) {
    return get_timespec_ns(tend) - get_timespec_ns(tstart);
}

u64 read_tsc_ns(u64 *now_ns) {
    struct timespec tspec;
    u64 tsc_start, tsc_end, retry = 0;
    do {
        tsc_start = _rdtsc();
        clock_gettime(CLOCK_REALTIME, &tspec);
        tsc_end = _rdtsc();
        retry += 1;
    } while (tsc_end - tsc_start > GETTIME_THRESH && retry <= 1000);

    *now_ns = get_timespec_ns(&tspec);
    return tsc_end;
}

u64 measure_freq_once(int sleep_us) {
    u64 tsc_start, tsc_end;
    u64 ns_start, ns_end;

    tsc_start = read_tsc_ns(&ns_start);
    usleep(sleep_us);
    tsc_end = read_tsc_ns(&ns_end);

    u64 ns_diff = ns_end - ns_start;
    u64 tsc_diff = tsc_end - tsc_start;
    return (u64)((double)tsc_diff / (double)ns_diff * 1e9);
}

int measure_rdtsc_freq(int num_measures, int sleep_us) {
    struct timespec tspec;
    u64 *freqs = calloc(num_measures, sizeof(u64));
    if (!freqs) return -ENOMEM;

    // check & warmup
    for (u32 i = 0; i < 20; i++) {
        if (clock_gettime(CLOCK_REALTIME, &tspec)) return -EINVAL;
        _timer_warmup();
    }

    for (int i = 0; i < num_measures; i++) {
        freqs[i] = measure_freq_once(sleep_us);
    }

    for (int i = 0; i < num_measures; i++) {
        printf("%lu ", freqs[i]);
    }
    free(freqs);
    return 0;
}

struct epoch_record {
    u64 tsc, epoch_ns, tsc_diff, core_id;
};

int get_tsc_and_time(u32 cnt) {
    struct timespec ts;
    struct epoch_record *records = calloc(cnt, sizeof(struct epoch_record));
    if (!records) return errno;

    // check & warmup
    for (u32 i = 0; i < 20; i++) {
        if (clock_gettime(CLOCK_REALTIME, &ts)) return -EINVAL;
        _timer_warmup();
    }

    for (u32 i = 0; i < cnt; i++) {
        u64 tsc = _timer_start();
        clock_gettime(CLOCK_REALTIME, &ts);
        u64 tsc_end = _timer_end();
        u32 aux;
        _rdtscp_aux(&aux);
        records[i].tsc = tsc_end;
        records[i].epoch_ns = get_timespec_ns(&ts);
        records[i].tsc_diff = tsc_end - tsc;
        records[i].core_id = aux;
    }

    for (u32 i = 0; i < cnt; i++) {
        printf("%lu %lu %lu %lu\n", records[i].tsc, records[i].epoch_ns,
               records[i].tsc_diff, records[i].core_id);
    }

    free(records);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) return -EINVAL;

    const char *s = getenv("GETTIME_THRESH");
    if (s) GETTIME_THRESH = strtoul(s, NULL, 10);

    if (strcmp(argv[1], "freq") == 0) {
        if (argc < 4) return -EINVAL;
        int num_measures = strtol(argv[2], NULL, 10);
        int sleep_us = strtol(argv[3], NULL, 10);
        return measure_rdtsc_freq(num_measures, sleep_us);
    } else if (strcmp(argv[1], "time") == 0) {
        u32 cnt = 1;
        if (argc == 3) cnt = strtol(argv[2], NULL, 10);
        return get_tsc_and_time(cnt);
    } else {
        return -EINVAL;
    }
}
