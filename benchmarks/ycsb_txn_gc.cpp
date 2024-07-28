#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_masstree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "garbage_collect.h"

enum CaseType { CASE1, CASE2, CASE3 };

void apply_case_config(CaseType case_type, ycsb_wl * wl, ycsb_query * query, uint32_t &transaction_size, uint32_t &thread_count, double &zipf_theta) {
    switch (case_type) {
        case CASE1:
            transaction_size = 20;
            zipf_theta = 0.0;
            // Vary thread count as needed
            break;
        case CASE2:
            transaction_size = 20;
            thread_count = 40;
            // Vary zipf_theta as needed
            break;
        case CASE3:
            thread_count = 40;
            zipf_theta = 0.0;
            // Control variation in transaction size
            break;
    }
    // Apply the settings to workload and query
    wl->set_zipf_theta(zipf_theta);
    query->set_transaction_size(transaction_size);
    query->set_thread_count(thread_count);
}

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
    RC rc;
    ycsb_query * m_query = (ycsb_query *) query;
    ycsb_wl * wl = (ycsb_wl *) h_wl;
    itemid_t * m_item = NULL;
    row_cnt = 0;

    // Determine the case type based on some criteria (e.g., input parameters or configuration)
    CaseType case_type = CASE1; // This should be set based on your specific scenario
    uint32_t transaction_size = 0;
    uint32_t thread_count = 0;
    double zipf_theta = 0.0;
    apply_case_config(case_type, wl, m_query, transaction_size, thread_count, zipf_theta);

    // Initialize variables for GC
    std::vector<int> local_epochs(thread_count);
    Block current_block;

    for (uint32_t rid = 0; rid < m_query->request_cnt; rid++) {
        ycsb_request * req = &m_query->requests[rid];
        int part_id = wl->key_to_part(req->key);
        bool finish_req = false;
        UInt32 iteration = 0;
        while (!finish_req) {
            if (iteration == 0) {
                m_item = index_read(_wl->the_index, req->key, part_id);
            }
#if INDEX_STRUCT == IDX_BTREE
            else {
                _wl->the_index->index_next(get_thd_id(), m_item);
                if (m_item == NULL)
                    break;
            }
#endif
            row_t * row = ((row_t *)m_item->location);
            row_t * row_local;
            access_t type = req->rtype;

            row_local = get_row(row, type);
            if (row_local == NULL) {
                rc = Abort;
                goto final;
            }

            // Computation //
            // Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
                    // for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;
                    char * data = row_local->get_data();
                    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
                    // }
                } else {
                    assert(req->rtype == WR);
                    // for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;
                    char * data = row->get_data();
                    *(uint64_t *)(&data[fid * 10]) = 0;
                    // }
                }
            }

            // Call CollectObsoleteBlock during transaction execution
            CollectObsoleteBlock(*this->h_thd, current_block, local_epochs);

            iteration++;
            if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
                finish_req = true;
        }
    }
    rc = RCOK;
    final:
    // Call CleanObsoleteBlock after transaction commit
    std::vector<Thread> all_threads; // Assuming you have a way to populate this with all threads
    CleanObsoleteBlock(*this->h_thd, *this, all_threads, local_epochs);

    rc = finish(rc);
    return rc;
}
