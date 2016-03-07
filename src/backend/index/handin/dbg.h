//
// Created by wendongli on 3/6/16.
//

#pragma once

#define P2DEBUG 1
#define dbg_msg(...) \
        do { if (P2DEBUG) { fprintf(stderr, "line %d: ", __LINE__); \
                            fprintf(stderr, __VA_ARGS__); \
                            fprintf(stderr, "\n"); \
                            fflush(stderr); } \
        } while (0)
