#pragma once

#include <vector>
#include <algorithm>
#include "block.h"
#include "thread.h"
#include "transaction.h"

void CollectObsoleteBlock(Thread& thread, Block& current_block, std::vector<int>& local_epochs);
void CleanObsoleteBlock(Thread& thread, Transaction& txn, std::vector<Thread>& all_threads, std::vector<int>& local_epochs);

#define MAX_LONG_TX_GC_QUEUE_SIZE 1024
#define MAX_LOCAL_GC_QUEUE_SIZE 1024

#endif // GARBAGE_COLLECT_H
