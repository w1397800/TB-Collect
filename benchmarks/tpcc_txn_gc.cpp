#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_masstree.h"
#include "tpcc_const.h"

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;
}

RC tpcc_txn_man::run_txn(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
	switch (m_query->type) {
		case TPCC_PAYMENT :
			return run_payment(m_query); break;
		case TPCC_NEW_ORDER :
			return run_new_order(m_query); break;
		case TPCC_ORDER_STATUS :
			return run_order_status(m_query); break;
		case TPCC_DELIVERY :
			return run_delivery(m_query); break;
		case TPCC_STOCK_LEVEL :
			return run_stock_level(m_query); break;
		default:
			assert(false);
	}
}

#include "garbage_collect.h"  // Include the garbage collection header

RC tpcc_txn_man::run_payment(tpcc_query * query) {
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;

    uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;


    /*====================================================+
        EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
        WHERE w_id=:w_id;
    +====================================================*/
    key = query->w_id;
    INDEX * index = _wl->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    if (g_wh_update)
        r_wh_local = get_row(r_wh, WR);
    else
        r_wh_local = get_row(r_wh, RD);

    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    double w_ytd;

    r_wh_local->get_value(W_YTD, w_ytd);
    if (g_wh_update) {
        // Collect obsolete blocks before updating warehouse
        CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
        r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
    }
    char w_name[11];
    char * tmp_str = r_wh_local->get_value(W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';

    /*=====================================================+
        EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +=====================================================*/
    key = distKey(query->d_id, query->d_w_id);
    item = index_read(_wl->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_dist = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }

    double d_ytd;
    r_dist_local->get_value(D_YTD, d_ytd);
    // Collect obsolete blocks before updating district
    CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
    r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
    char d_name[11];
    tmp_str = r_dist_local->get_value(D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';

    /*====================================================================+
        EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
        INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
        FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/

    row_t * r_cust;
    if (query->by_last_name) {
        uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
        INDEX * index = _wl->i_customer_last;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);

        int cnt = 0;
        itemid_t * it = item;
        itemid_t * mid = item;
        while (it != NULL) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0)
                mid = mid->next;
        }
        r_cust = ((row_t *)mid->location);
    } else {
        key = custKey(query->c_id, query->c_d_id, query->c_w_id);
        INDEX * index = _wl->i_customer_id;
        item = index_read(index, key, wh_to_part(c_w_id));
        assert(item != NULL);
        r_cust = (row_t *) item->location;
    }

    /*======================================================================+
        EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
        WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
    +======================================================================*/
    row_t * r_cust_local = get_row(r_cust, WR);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    r_cust_local->get_value(C_BALANCE, c_balance);
    // Collect obsolete blocks before updating customer
    CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
    r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
    r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
    r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
    r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
    r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    char * c_credit = r_cust_local->get_value(C_CREDIT);

    if (strstr(c_credit, "BC")) {
        // Special handling for bad credit customers
    }

    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = strlen(h_data);
    if (length > 10) length = 10;
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length + 14] = '\0';

    // Collect obsolete blocks before finishing the transaction
    CleanObsoleteBlock(*_curr_thread, *this, _all_threads, _local_epochs);

    assert(rc == RCOK);
    return finish(rc);
}



#include "garbage_collect.h"  // Include the garbage collection header

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
    RC rc = RCOK;
    uint64_t key;
    itemid_t * item;
    INDEX * index;

    bool remote = query->remote;
    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;


    /*=======================================================================+
    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
        INTO :c_discount, :c_last, :c_credit, :w_tax
        FROM customer, warehouse
        WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    +========================================================================*/
    key = w_id;
    index = _wl->i_warehouse;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local = get_row(r_wh, RD);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }

    double w_tax;
    r_wh_local->get_value(W_TAX, w_tax);
    key = custKey(c_id, d_id, w_id);
    index = _wl->i_customer_id;
    item = index_read(index, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_cust = (row_t *) item->location;
    row_t * r_cust_local = get_row(r_cust, RD);
    if (r_cust_local == NULL) {
        return finish(Abort);
    }
    uint64_t c_discount;
    r_cust_local->get_value(C_DISCOUNT, c_discount);

    /*==================================================+
    EXEC SQL SELECT d_next_o_id, d_tax
        INTO :d_next_o_id, :d_tax
        FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
    EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
        WHERE d_id = :d_id AND d_w_id = :w_id;
    +===================================================*/
    key = distKey(d_id, w_id);
    item = index_read(_wl->i_district, key, wh_to_part(w_id));
    assert(item != NULL);
    row_t * r_dist = ((row_t *)item->location);
    row_t * r_dist_local = get_row(r_dist, WR);
    if (r_dist_local == NULL) {
        return finish(Abort);
    }
    int64_t o_id;
    o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
    o_id++;
    // Collect obsolete blocks before updating district
    CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
    r_dist_local->set_value(D_NEXT_O_ID, o_id);

    for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
        uint64_t ol_i_id = query->items[ol_number].ol_i_id;
        uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
        uint64_t ol_quantity = query->items[ol_number].ol_quantity;

        /*===========================================+
        EXEC SQL SELECT i_price, i_name, i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        key = ol_i_id;
        item = index_read(_wl->i_item, key, 0);
        assert(item != NULL);
        row_t * r_item = ((row_t *)item->location);

        row_t * r_item_local = get_row(r_item, RD);
        if (r_item_local == NULL) {
            return finish(Abort);
        }
        int64_t i_price;
        r_item_local->get_value(I_PRICE, i_price);

        /*===================================================================+
        EXEC SQL SELECT s_quantity, s_data,
                s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
            INTO :s_quantity, :s_data,
                :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
                :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
            FROM stock
            WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
        EXEC SQL UPDATE stock SET s_quantity = :s_quantity
            WHERE s_i_id = :ol_i_id
            AND s_w_id = :ol_supply_w_id;
        +===============================================*/

        uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
        INDEX * stock_index = _wl->i_stock;
        itemid_t * stock_item;
        index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
        assert(item != NULL);
        row_t * r_stock = ((row_t *)stock_item->location);
        row_t * r_stock_local = get_row(r_stock, WR);
        if (r_stock_local == NULL) {
            return finish(Abort);
        }

        UInt64 s_quantity;
        int64_t s_remote_cnt;
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
        int64_t s_ytd;
        int64_t s_order_cnt;
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
#endif
        if (remote) {
            s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
            s_remote_cnt++;
            // Collect obsolete blocks before updating stock
            CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
            r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
        }
        uint64_t quantity;
        if (s_quantity > ol_quantity + 10) {
            quantity = s_quantity - ol_quantity;
        } else {
            quantity = s_quantity - ol_quantity + 91;
        }
        // Collect obsolete blocks before updating stock
        CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
        r_stock_local->set_value(S_QUANTITY, &quantity);
    }

    // Collect obsolete blocks before finishing the transaction
    CleanObsoleteBlock(*_curr_thread, *this, _all_threads, _local_epochs);

    assert(rc == RCOK);
    return finish(rc);
}




RC 
tpcc_txn_man::run_order_status(tpcc_query * query) {
	row_t * r_cust;
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		//	   	EXEC SQL FETCH c_name
		//	   	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		uint64_t thd_id = get_thd_id();
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		r_cust = (row_t *) item->location;
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	INDEX * index = _wl->i_order;
	itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	row_t * r_order = (row_t *) item->location;
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		assert(false); 
		return finish(Abort);
	}

	uint64_t o_id, o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	key = orderlineKey(query->w_id, query->d_id, o_id);
	index = _wl->i_orderline;
	item = index_read(index, key, wh_to_part(query->w_id));
	assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}
#endif

final:
	assert( rc == RCOK );
	return finish(rc)
	return RCOK;
}

#include "garbage_collect.h"  // Include the garbage collection header

RC tpcc_txn_man::run_delivery(tpcc_query * query) {
    // XXX HACK if another delivery txn is running on this warehouse, simply commit.
    if (!ATOM_CAS(_wl->delivering[query->w_id], false, true))
        return finish(RCOK);

    for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
        uint64_t key = distKey(d_id, query->w_id);
        INDEX * index = _wl->i_orderline_wd;
        itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
        assert(item != NULL);
        while (item->next != NULL) {
#if DEBUG_ASSERT
            uint64_t o_id_1, o_id_2;
            ((row_t *)item->location)->get_value(OL_O_ID, o_id_1);
            ((row_t *)item->next->location)->get_value(OL_O_ID, o_id_2);
            assert(o_id_1 > o_id_2);
#endif
            item = item->next;
        }
        uint64_t no_o_id;
        row_t * r_orderline = (row_t *)item->location;
        r_orderline->get_value(OL_O_ID, no_o_id);
        // TODO the orderline row should be removed from the table and indexes.

        index = _wl->i_order;
        key = orderPrimaryKey(query->w_id, d_id, no_o_id);
        itemid_t * item_order = index_read(index, key, wh_to_part(query->w_id));
        row_t * r_order = (row_t *)item_order->location;
        row_t * r_order_local = get_row(r_order, WR);

        if (r_order_local == NULL) {
            return finish(Abort);
        }

        uint64_t o_c_id;
        r_order_local->get_value(O_C_ID, o_c_id);
        // Collect obsolete blocks before updating the order
        CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
        r_order_local->set_value(O_CARRIER_ID, query->o_carrier_id);

        itemid_t * item_orderline = index_read(_wl->i_order_line, orderlineKey(query->w_id, d_id, no_o_id));
        double sum_ol_amount = 0;
        double ol_amount;
        while (item_orderline != NULL) {
            row_t * r_orderline = (row_t *)item_orderline->location;
            if (r_orderline == NULL) {
                return finish(Abort);
            }
            // Collect obsolete blocks before updating the orderline
            CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
            r_orderline->set_value(OL_DELIVERY_D, query->ol_delivery_d);
            r_orderline->get_value(OL_AMOUNT, ol_amount);
            sum_ol_amount += ol_amount;
            item_orderline = item_orderline->next;
        }

        key = custKey(o_c_id, d_id, query->w_id);
        itemid_t * item_customer = index_read(_wl->i_customer_id, key, wh_to_part(query->w_id));
        row_t * r_cust = (row_t *)item_customer->location;
        row_t * r_cust_local = get_row(r_cust, WR);

        if (r_cust_local == NULL) {
            return finish(Abort);
        }

        double c_balance;
        uint64_t c_delivery_cnt;
        r_cust_local->get_value(C_BALANCE, c_balance);
        r_cust_local->get_value(C_DELIVERY_CNT, c_delivery_cnt);
        // Collect obsolete blocks before updating the customer
        CollectObsoleteBlock(*_curr_thread, *_curr_block, _local_epochs);
        r_cust_local->set_value(C_BALANCE, c_balance + sum_ol_amount);
        r_cust_local->set_value(C_DELIVERY_CNT, c_delivery_cnt + 1);
    }

    // Clean obsolete blocks at the end of the transaction
    CleanObsoleteBlock(*_curr_thread, *this, _all_threads, _local_epochs);

    return finish(RCOK);
}



RC 
tpcc_txn_man::run_stock_level(tpcc_query * query) {

    row_t * r_cust;
    if (query->by_last_name) {
        uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
        // XXX: the list is not sorted. But let's assume it's sorted...
        // The performance won't be much different.
        INDEX * index = _wl->i_customer_last;
        uint64_t thd_id = get_thd_id();
        itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
        int cnt = 0;
        itemid_t * it = item;
        itemid_t * mid = item;
        while (it != NULL) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0)
                mid = mid->next;
        }
        r_cust = ((row_t *)mid->location);
    } else {
        uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
        INDEX * index = _wl->i_customer_id;
        itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
        r_cust = (row_t *) item->location;
    }
#if TPCC_ACCESS_ALL

    row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
    uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
    INDEX * index = _wl->i_order;
    itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
    row_t * r_order = (row_t *) item->location;
    row_t * r_order_local = get_row(r_order, RD);
    if (r_order_local == NULL) {
        assert(false);
        return finish(Abort);
    }

    uint64_t o_id, o_entry_d, o_carrier_id;
    r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
    r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
    itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

    key = orderlineKey(query->w_id, query->d_id, o_id);
    index = _wl->i_orderline;
    item = index_read(index, key, wh_to_part(query->w_id));
    assert(item != NULL);

	return RCOK;
}
