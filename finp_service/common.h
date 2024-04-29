#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef float f32;
typedef double f64;


#define _TEST_BIT(data, bit) (!!((data) & (1ull << (bit))))

static __always_inline u64 _count_ones(u64 val) {
    u64 ret;
    __asm__ __volatile__("popcnt %1, %0" : "=r"(ret) : "r"(val) : "cc");
    return ret;
}

// Timer releated ASM
// ====== timer related ======
static __always_inline u64 _rdtscp(void) {
    u64 rax;
    __asm__ __volatile__(
        "rdtscp\n\t"
        "shl $32, %%rdx\n\t"
        "or %%rdx, %0\n\t"
        :"=a"(rax)
        :: "rcx", "rdx", "memory", "cc"
    );
    return rax;
}

static __always_inline u64 _rdtscp_aux(u32 *aux) {
    u64 rax;
    __asm__ __volatile__(
        "rdtscp\n\t"
        "shl $32, %%rdx\n\t"
        "or %%rdx, %0\n\t"
        "mov %%ecx, %1\n\t"
        :"=a"(rax), "=r"(*aux)
        :: "rcx", "rdx", "memory", "cc"
    );
    return rax;
}

static __always_inline u64 _rdtsc(void) {
    u64 rax;
    __asm__ __volatile__(
        "rdtsc\n\t"
        "shl $32, %%rdx\n\t"
        "or %%rdx, %0\n\t"
        :"=a"(rax)
        :: "rdx", "memory", "cc"
    );
    return rax;
}

// https://github.com/google/highwayhash/blob/master/highwayhash/tsc_timer.h
static __always_inline u64 _rdtsc_google_begin(void) {
    u64 t;
    __asm__ __volatile__("mfence\n\t"
                         "lfence\n\t"
                         "rdtsc\n\t"
                         "shl $32, %%rdx\n\t"
                         "or %%rdx, %0\n\t"
                         "lfence"
                         : "=a"(t)
                         :
                         // "memory" avoids reordering. rdx = TSC >> 32.
                         // "cc" = flags modified by SHL.
                         : "rdx", "memory", "cc");
    return t;
}

static __always_inline u64 _rdtscp_google_end(void) {
    u64 t;
    __asm__ __volatile__("rdtscp\n\t"
                         "shl $32, %%rdx\n\t"
                         "or %%rdx, %0\n\t"
                         "lfence"
                         : "=a"(t)
                         :
                         // "memory" avoids reordering.
                         // rcx = TSC_AUX. rdx = TSC >> 32.
                         // "cc" = flags modified by SHL.
                         : "rcx", "rdx", "memory", "cc");
    return t;
}

static __always_inline u64 _rdtscp_google_end_aux(u32 *aux) {
    u64 t;
    __asm__ __volatile__("rdtscp\n\t"
                         "shl $32, %%rdx\n\t"
                         "or %%rdx, %0\n\t"
                         "lfence"
                         : "=a"(t), "=c"(*aux)
                         :
                         // "memory" avoids reordering.
                         // rcx = TSC_AUX. rdx = TSC >> 32.
                         // "cc" = flags modified by SHL.
                         : "rdx", "memory", "cc");
    return t;
}

// use google's method by default
#define _timer_start _rdtsc_google_begin
#define _timer_end   _rdtscp_google_end
#define _timer_end_aux   _rdtscp_google_end_aux

static __always_inline u64 _timer_warmup(void) {
    u64 lat = _timer_start();
    lat = _timer_end() - lat;
    return lat;
}
