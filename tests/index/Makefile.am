## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# INDEX
######################################################################

check_PROGRAMS += \
                bwtree_index_test \
                index_test \
                garbage_collector_test \
                pid_table_test \
                index_test_modified

index_test_SOURCES = index/index_test.cpp \
                     harness.cpp

pid_table_test_SOURCES = index/pid_table_test.cpp \
                         harness.cpp

garbage_collector_test_SOURCES = index/garbage_collector_test.cpp \
                                 harness.cpp

bwtree_index_test_SOURCES = index/bwtree_index_test.cpp \
                            harness.cpp

index_test_modified_SOURCES = index/index_test_modified.cpp \
                     harness.cpp