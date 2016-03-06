//
// Created by wendongli on 3/6/16.
//

#pragma once

#define P2DEBUG 1
#define dbg_msg(fmt, ...) \
        do { if (P2DEBUG) { fprintf(stderr, "line %d: " fmt, \
                                __LINE__, __VA_ARGS__); \
                          fflush(stderr); } \
        } while (0)
