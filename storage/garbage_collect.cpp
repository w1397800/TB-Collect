#include "garbage_collect.h"
#include <libpmemobj.h>
#include <algorithm>

void CollectObsoleteBlock(Thread& thread, Block& current_block, std::vector<int>& local_epochs) {
    // Check if update_map has more than 90% 1s
    if (std::count(current_block.update_map.begin(), current_block.update_map.end(), 1) > 0.9 * current_block.update_map.size()) {
        // Add current block to local thread's pre-recycle queue
        thread.gc_pending_queue.push_back(current_block);

        // Traverse through the pre-recycle queue
        for (auto& block : thread.gc_pending_queue) {
            // Find the minimum local epoch
            int local_epoch_min = *std::min_element(local_epochs.begin(), local_epochs.end());

            // If the earliest active transaction is later than the last write transaction in the block
            if (local_epoch_min > block.max_txnid) {
                // Add the block to the local GC queue
                thread.local_gc_queue.push_back(block);
            }
                // If the earliest active transaction is earlier than the first write transaction in the block
            else if (local_epoch_min < block.min_txnid) {
                // Loop to further determine
                bool long_tx_detected = false;
                for (size_t i = 1; i < local_epochs.size(); ++i) {
                    int local_epoch_next_min = *std::min_element(local_epochs.begin() + i, local_epochs.end());

                    if (local_epoch_next_min > block.max_txnid) {
                        // Long transaction detected, add block to long transaction GC queue
                        thread.long_tx_gc_queue.push_back(block);
                        long_tx_detected = true;
                        break;
                    } else if (local_epoch_next_min < block.min_txnid) {
                        // Check the next epoch
                        continue;
                    }
                }

                // If a long transaction was detected
                if (long_tx_detected) {
                    // Mark the transaction as long transaction
                    thread.long_tx_gc_queue.push_back(block);
                }
            }
        }
    }
}

void CleanObsoleteBlock(Thread& thread, Transaction& txn, std::vector<Thread>& all_threads, std::vector<int>& local_epochs) {
    int local_epoch_min = *std::min_element(local_epochs.begin(), local_epochs.end());

    // If the transaction that just ended is a long transaction
    if (txn.is_long_tx) {
        // Scan all threads' long_tx_gc_queue
        for (auto& other_thread : all_threads) {
            for (auto& block : other_thread.long_tx_gc_queue) {
                // If block.max_txnid is less than local_epoch_min
                if (block.max_txnid < local_epoch_min) {
                    // Add block to the local GC queue
                    thread.local_gc_queue.push_back(block);
                }
            }
        }
    }

    // If the long_tx_gc_queue is full
    if (thread.long_tx_gc_queue.size() >= MAX_LONG_TX_GC_QUEUE_SIZE) {
        // Recycle the blocks and release NVM storage space, retain tiles in DRAM
        for (auto& block : thread.long_tx_gc_queue) {
            // Release NVM storage space
            pmemobj_free(&block);
        }
        // Clear the long_tx_gc_queue
        thread.long_tx_gc_queue.clear();
    }

    // If the local_gc_queue is full
    if (thread.local_gc_queue.size() >= MAX_LOCAL_GC_QUEUE_SIZE) {
        // Freeze the current local GC queue
        thread.local_gc_queue_frozen = thread.local_gc_queue;
        thread.local_gc_queue.clear();

        // For each block in the frozen local GC queue
        for (auto& block : thread.local_gc_queue_frozen) {
            Block new_block;

            // Iterate through the update_map to find the latest tuples
            for (size_t i = 0; i < block.update_map.size(); ++i) {
                if (block.update_map[i] == 0) {
                    // The tuple at position i is the latest version
                    new_block.tuples.push_back(block.tuples[i]);
                }
            }

            // Insert the latest versions into a new block N+1
            InsertNewBlock(new_block);

            // Release the NVM space of the block
            pmemobj_free(&block);
        }

        // Clear the frozen local GC queue
        thread.local_gc_queue_frozen.clear();
    }
}
