/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2007      The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2011-2020 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC. All
 *                         rights reserved.
 * Copyright (c) 2014-2020 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include "prte_config.h"
#include "constants.h"
#include "types.h"

#include <string.h>

#include "src/dss/dss.h"
#include "src/class/prte_list.h"
#include "src/pmix/pmix-internal.h"
#include "src/mca/prtecompress/prtecompress.h"

#include "src/mca/errmgr/errmgr.h"
#include "src/mca/rml/base/base.h"
#include "src/mca/rml/base/rml_contact.h"
#include "src/mca/routed/base/base.h"
#include "src/mca/state/state.h"
#include "src/util/name_fns.h"
#include "src/util/nidmap.h"
#include "src/util/proc_info.h"

#include "src/mca/grpcomm/base/base.h"

#include "grpcomm_ucx.h"

#include "ucp/api/ucp.h"
#include "ucg/api/ucg.h"
#include "ucg/api/ucg_mpi.h"
#include "ucg/api/ucg_minimal.h"

//#include "mpi.h"

/* Receive buffer size for xcast */
#define GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE (1024*1024)

/* Actual send size offset in the large buffer (we send 1024 bytes of Bcast buffer) */
#define XCAST_BUFFER_SEND_SIZE_OFFSET (GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE - sizeof(size_t))

/* UCX Bcast Handshake string */
#define BCAST_HANDSHAKE_STRING "UCG_BCAST_HANDSHAKE"

/* Maximum size of Node IP address string */
#define NODE_IP_ADDRESS_STR_SIZE 64

#define CHKERR_ACTION(_cond, _msg, _action) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s\n", _msg); \
            _action; \
        } \
    } while (0)

#define CHKERR_JUMP(_cond, _msg, _label) \
    CHKERR_ACTION(_cond, _msg, goto _label)

#define ARRAY_SIZE(aRRAY) (sizeof(aRRAY) / sizeof(aRRAY[0]))

#define HAVE_UCG

//OMPI_DECLSPEC extern struct ompi_predefined_datatype_t ompi_mpi_byte;

/*
ucp_context_h g_ucp_context;
ucp_worker_h  g_ucp_worker;
ucp_address_t *g_address;
size_t        g_address_length;
*/

/* internal variables */
static prte_list_t tracker;

/* Bcast handshake successful */
static volatile int grpcomm_ucx_bcast_handshake_success = 0;

/* Global root node address (used for broadcast over UCX) */
static int grpcomm_ucx_lateinit_done = 0;

static ucp_params_t ucp_params;

/* UCG-minimal context */
ucg_minimal_ctx_t g_ucg_context;

/* Static API's */
static int init(void);
static void finalize(void);
static int xcast(prte_vpid_t *vpids,
                 size_t nprocs,
                 prte_buffer_t *buf);
static int allgather(prte_grpcomm_coll_t *coll,
                     prte_buffer_t *buf, int mode);

/* Module def */
prte_grpcomm_base_module_t prte_grpcomm_ucx_module = {
    .init = init,
    .finalize = finalize,
    .xcast = xcast,
    .allgather = allgather,
    .rbcast = NULL,
    .register_cb = NULL,
    .unregister_cb = NULL
};

/* internal functions */
static void xcast_recv(int status, prte_process_name_t* sender,
                       prte_buffer_t* buffer, prte_rml_tag_t tag,
                       void* cbdata);
static void allgather_recv(int status, prte_process_name_t* sender,
                           prte_buffer_t* buffer, prte_rml_tag_t tag,
                           void* cbdata);
static void barrier_release(int status, prte_process_name_t* sender,
                            prte_buffer_t* buffer, prte_rml_tag_t tag,
                            void* cbdata);

static int grpcomm_ucx_lateinit(int is_root_node, char *root_node_address, unsigned num_connections);

/* internal variables */
static prte_list_t tracker;

static uint16_t server_port     = 13437;
static long test_string_length  = 64;

/* Broadcast thread id */
static pthread_t grpcomm_ucx_bcast_tid;

/* Finalize thread */
volatile int service_finalize = 0;

/* Receive buffer for xcast */
static uint64_t grpcomm_xcast_recv_buffer[GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE/sizeof(uint64_t)];

/* Send buffer for xcast */
static uint64_t grpcomm_xcast_send_buffer[GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE/sizeof(uint64_t)];

static void grpcomm_ucx_root_node_find(char *root_node_ip, size_t *number_of_nodes, int *is_this_node_root)
{
    /* "name" itself might be an alias, so find the node object for this name */
    char **n2names = NULL;
    char *n2alias = NULL;
    char **n1names = NULL;
    char *n1alias = NULL;
    int i, m;
    prte_node_t *nptr;
    size_t num_nodes = 0;
    //int root_node_initialized = 0;
    const char *root_node_ip_addr = "192.168.20.100"; //getenv("PRTE_GRPCOMM_UCX_ROOT_NODE_IP");
    static struct ifreq ifreqs[32];
    struct ifconf ifconf;

    prte_output(0, "root_node_find(1), root_node_ip_addr=%p\n", root_node_ip_addr);

    *number_of_nodes = (size_t)atoi("2" /*getenv("PRTE_GRPCOMM_UCX_NUM_NODES")*/);

    sprintf(root_node_ip, "%s", root_node_ip_addr);

    prte_output(0, "root_node_find(2)\n");

    memset(&ifconf, 0, sizeof(ifconf));
    ifconf.ifc_req = ifreqs;
    ifconf.ifc_len = sizeof(ifreqs);

    int sd = socket(PF_INET, SOCK_STREAM, 0);
    assert(sd >= 0);

    int r = ioctl(sd, SIOCGIFCONF, (char *)&ifconf);
    assert(r == 0);

        prte_output(0, "root_node_find(3)\n");

    *is_this_node_root = 0;
    for(int i = 0; i < ifconf.ifc_len/sizeof(struct ifreq); ++i)
    {
        const char *iface_ip_address = inet_ntoa(((struct sockaddr_in *)&ifreqs[i].ifr_addr)->sin_addr);
        if (strcmp(root_node_ip, iface_ip_address) == 0) {
            *is_this_node_root = 1;
        }

        printf("%s: %s\n", ifreqs[i].ifr_name, iface_ip_address);
    }

    prte_output(0, "root_node_find(4)\n");

    close(sd);

#if 0
    for (i = 0; i < prte_node_pool->size; i++) {
        if (NULL == (nptr = (prte_node_t*)prte_pointer_array_get_item(prte_node_pool, i))) {
            continue;
        }

        prte_output(0, "index=%d : ", i);

        if (prte_get_attribute(&nptr->attributes, PRTE_NODE_ALIAS, (void**)&n2alias, PRTE_STRING)) {
            prte_output(0, " %s", n2alias);
            //n2names = prte_argv_split(n2alias, ',');
        }

        char *ch;
        ch = strtok(n2alias, ",");
        m = 0;
        while (ch != NULL) {
            prte_output(0, " [m=%d]:%s", m, ch);

            /* Initialize the IP address of the root node */
            if (strcmp(root_node_ip,
            if ((!root_node_initialized) && (m == 3)) {
                sprintf(root_node_ip, "%s", ch);
                root_node_initialized = 1;
            }

            ch = strtok(NULL, " ,");
            m++;
        }

        prte_output(0, "\n");

        free(n2alias);

        if (n2names != NULL) {
            prte_argv_free(n2names);
        }

        n2names = NULL;

        num_nodes++;
    }
#endif

    //*number_of_nodes = num_nodes;

}

static void *grpcomm_ucx_bcast_thread(void *arg)
{
    int is_root_node;
    int ret;
    pthread_t id = pthread_self();
    char root_node_ip[NODE_IP_ADDRESS_STR_SIZE];
    size_t number_of_nodes;
    ucs_status_t status;
    int temp;

    //prte_proc_info();

    //usleep(1000000);
    prte_output(0, "ucx ==> xcast()\n");

    /* Scan nodes and find root */
    grpcomm_ucx_root_node_find(root_node_ip, &number_of_nodes, &is_root_node);
    prte_output(0, "ucx ==> xcast(), root_node_ip = %s, number_of_nodes = %u, is_root_node=%d\n",
           root_node_ip, number_of_nodes, is_root_node);

    prte_output(0, "ucx ==> xcast(1)\n");

    /* Initialize the UCG-broadcast */
    ret = grpcomm_ucx_lateinit(is_root_node, root_node_ip, (number_of_nodes - 1));
    if (ret != PRTE_SUCCESS) {
        prte_output(0, "ucx ==> xcast() gpcomm_ucx_lateinit() failed, ret=%d\n", ret);
        return ret;
    }

    if (is_root_node) {
        snprintf(grpcomm_xcast_recv_buffer, sizeof(grpcomm_xcast_recv_buffer), "%s", BCAST_HANDSHAKE_STRING);
    }

    prte_output(0, "ucx ==> xcast(2)\n");

    /* Only relevant for more than 1 node */
    prte_output(0, "number_of_nodes = %d, is_root_node=%d\n", number_of_nodes, is_root_node);
    if (number_of_nodes == 1) {
        return NULL;
    }

    prte_output(0, "ucx ==> xcast(3)\n");

    while (!service_finalize) {
        prte_output(0, "Calling UCX Bcast...\n");

        status = ucg_minimal_broadcast(&g_ucg_context, grpcomm_xcast_recv_buffer,
                     sizeof(grpcomm_xcast_recv_buffer));
        if (status != UCS_OK) {
            prte_output(0, "ucx ==> xcast() ucg_minimal_broadcast() failed, status=%d\n", status);
        }

        prte_output(0, "Bcast successful!\n");

        if (grpcomm_ucx_bcast_handshake_success == 0) {
            /* Bcast handshake success */
            grpcomm_ucx_bcast_handshake_success = 1;
        }
        else {
            /* now pass the relay buffer to myself for processing - don't
             * inject it into the RML system via send as that will compete
             * with the relay messages down in the OOB. Instead, pass it
             * ucxly to the RML message processor */
            size_t recv_size = *(size_t *)(&grpcomm_xcast_recv_buffer[XCAST_BUFFER_SEND_SIZE_OFFSET]);
            prte_output(0, "Bcast thread - Posting received buffer to user, size=%zu\n", recv_size);
            PRTE_RML_POST_MESSAGE(PRTE_PROC_MY_NAME, PRTE_RML_TAG_XCAST, 1,
                    grpcomm_xcast_recv_buffer, recv_size);
        }

#if 0 // debug code
       grpcomm_xcast_recv_buffer[30] = 0x00;
        prte_output(0, "UCX Bcast received status=%d, received string=%s\n", status,
                       grpcomm_xcast_recv_buffer);
#endif

        /* Root just sends the initial handshake */
        if (is_root_node) {
            prte_output(0, "Bcast thread: Root node ==> exiting thread...\n");
            return NULL;
        }
    }

    return NULL;
}

static void grpcomm_ucg_finalize(void)
{
}

static int grpcomm_ucx_lateinit(int is_root_node, char *root_node_address, unsigned num_connections)
{
    int ret = PRTE_ERROR;
    ucs_sock_addr_t server_address = { 0 };
    prte_job_t *jdata;
    prte_app_context_t *dapp;
    ucs_status_t status;

    if (!grpcomm_ucx_lateinit_done) {
        prte_output(0, "ucx ==> init() PRTE_NAME_PRINT(PRTE_PROC_MY_NAME)=%s\n",
            PRTE_NAME_PRINT(PRTE_PROC_MY_NAME));

#if 0
        /* get the daemon job object - was created by ess/hnp component */
       if (NULL == (jdata = prte_get_job_data_object(PRTE_PROC_MY_NAME->jobid))) {
           prte_output(0, "ucx ==> init() Error! prte_get_job_data_object() failed!\n");
           return PRTE_ERROR;
       }

       /* ess/hnp also should have created a daemon "app" */
       if (NULL == (dapp = (prte_app_context_t*)prte_pointer_array_get_item(jdata->apps, 0))) {
           prte_output(0, "ucx ==> init() Error! prte_pointer_array_get_item() failed!\n");
           return PRTE_ERROR;
       }

        /* now filter the list through any -host specification */
        if (prte_get_attribute(&dapp->attributes, PRTE_APP_ROOT_NODE,
                (void**)&root_node_address, PRTE_STRING)) {
            prte_output(0, "ucx ==> init() PRTE root node: %s\n", root_node_address);
        }
#endif

        prte_output(0, "root_node_address = %s\n", root_node_address);

       // if (is_root_node) /*(PRTE_PROC_MY_NAME->vpid == 0)*/ {
       //else {
          // usleep(100000);
       //}

        /* TODO: Update the IP address of the root */
        //root_name = "127.0.0.1";

        //test_string = mem_type_malloc(test_string_length);
        //CHKERR_JUMP(test_string == NULL, "allocate memory\n", err);

        /* Is client mode? */
        ucg_minimal_ctx_t ctx;
        struct sockaddr_in sock_addr   = {
                .sin_family            = AF_INET,
                .sin_port              = htons(server_port),
                .sin_addr              = {
                        .s_addr        = root_node_address ?
                                             inet_addr(root_node_address) : INADDR_ANY
                }
        };
        ucs_sock_addr_t server_address = {
                .addr                  = (struct sockaddr *)&sock_addr,
                .addrlen               = sizeof(struct sockaddr)
        };

        //CHKERR_JUMP(sock_addr.sin_addr.s_addr == (uint32_t)-1, "lookup IP\n", err);

        server_address.addr = (struct sockaddr *)&sock_addr;
        server_address.addrlen = sizeof(struct sockaddr);

        prte_output(0, "Calling ucg_minimal_init()...\n");

        status = ucg_minimal_init(&g_ucg_context, &server_address, num_connections,
                     is_root_node ? UCG_MINIMAL_FLAG_SERVER : 0);
        CHKERR_JUMP(status != UCS_OK, "ucg_minimal_init\n", err_cleanup);

        prte_output(0, "ucg_minimal_init() - done\n");

        ret = PRTE_SUCCESS;

        grpcomm_ucx_lateinit_done = 1;

        goto grpcomm_ucx_lateinit_done_no_error;
    }

err_cleanup:
    ucg_minimal_finalize(&g_ucg_context);
    //mem_type_free(test_string);
    grpcomm_ucg_finalize();

grpcomm_ucx_lateinit_done_no_error:
    return ret;
}

/**
 * Initialize the module
 */
static int init(void)
{
    int err;

   /* prte_list_item_t *item, *next;
    prte_list_foreach_safe(foo, next, list, prte_list_item_t) {
       interface = foo
       prte_output(0, "ucx ==> init() item\n", item->);
    }
*/
    PRTE_CONSTRUCT(&tracker, prte_list_t);

    /* post the receives */
    prte_rml.recv_buffer_nb(PRTE_NAME_WILDCARD,
                            PRTE_RML_TAG_XCAST,
                            PRTE_RML_PERSISTENT,
                            xcast_recv, NULL);
    prte_rml.recv_buffer_nb(PRTE_NAME_WILDCARD,
                            PRTE_RML_TAG_ALLGATHER_UCX,
                            PRTE_RML_PERSISTENT,
                            allgather_recv, NULL);
    /* setup recv for barrier release */
    prte_rml.recv_buffer_nb(PRTE_NAME_WILDCARD,
                            PRTE_RML_TAG_COLL_RELEASE,
                            PRTE_RML_PERSISTENT,
                            barrier_release, NULL);

    /* Create the UCX Bcast thread */
    err = pthread_create(&grpcomm_ucx_bcast_tid, NULL,
              &grpcomm_ucx_bcast_thread, NULL);
    if (err != 0) {
        prte_output(0, "Error! can't create grpcomm_ucx_bcast thread :[%s]", strerror(err));
    }

    return PRTE_SUCCESS;
}

/**
 * Finalize the module
 */
static void finalize(void)
{
    PRTE_LIST_DESTRUCT(&tracker);

    ucg_minimal_finalize(&g_ucg_context);

    //mem_type_free(test_string);

    grpcomm_ucg_finalize();

    return;
}


static int xcast(prte_vpid_t *vpids, size_t nprocs, prte_buffer_t *buf)
{
    int rc;

    int ret;
    ucs_status_t status;

    prte_output(0, "grpcomm_ucx ==> %s entry, nprocs=%zu\n", __func__, nprocs);

    /* send it to the HNP (could be myself) for relay */
    PRTE_RETAIN(buf);  // we'll let the RML release it
    if (0 > (rc = prte_rml.send_buffer_nb(PRTE_PROC_MY_HNP, buf, PRTE_RML_TAG_XCAST,
                                          prte_rml_send_callback, NULL))) {
        PRTE_ERROR_LOG(rc);
        PRTE_RELEASE(buf);
        return rc;
    }
    return PRTE_SUCCESS;
}

static int allgather(prte_grpcomm_coll_t *coll,
                     prte_buffer_t *buf, int mode)
{
    int rc;
    prte_buffer_t *relay;

    prte_output(0, "grpcomm_ucx ==> %s entry\n", __func__);

    PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx: allgather",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME)));

    /* the base functions pushed us into the event library
     * before calling us, so we can safely access global data
     * at this point */

    relay = PRTE_NEW(prte_buffer_t);
    /* pack the signature */
    if (PRTE_SUCCESS != (rc = prte_dss.pack(relay, &coll->sig, 1, PRTE_SIGNATURE))) {
        PRTE_ERROR_LOG(rc);
        PRTE_RELEASE(relay);
        return rc;
    }

    /* pack the mode */
    if (PRTE_SUCCESS != (rc = prte_dss.pack(relay, &mode, 1, PRTE_INT))) {
        PRTE_ERROR_LOG(rc);
        PRTE_RELEASE(relay);
        return rc;
    }

    /* pass along the payload */
    prte_dss.copy_payload(relay, buf);

    /* send this to ourselves for processing */
    PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx:allgather sending to ourself",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME)));

    /* send the info to ourselves for tracking */
    rc = prte_rml.send_buffer_nb(PRTE_PROC_MY_NAME, relay,
                                 PRTE_RML_TAG_ALLGATHER_UCX,
                                 prte_rml_send_callback, NULL);
    return rc;
}

static void allgather_recv(int status, prte_process_name_t* sender,
                           prte_buffer_t* buffer, prte_rml_tag_t tag,
                           void* cbdata)
{
    int32_t cnt;
    int rc, ret, mode;
    prte_grpcomm_signature_t *sig;
    prte_buffer_t *reply;
    prte_grpcomm_coll_t *coll;

    prte_output(0, "grpcomm_ucx ==> %s entry, tag=%u\n", __func__, tag);

    PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx allgather recvd from %s",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME),
                         PRTE_NAME_PRINT(sender)));

    /* unpack the signature */
    cnt = 1;
    if (PRTE_SUCCESS != (rc = prte_dss.unpack(buffer, &sig, &cnt, PRTE_SIGNATURE))) {
        PRTE_ERROR_LOG(rc);
        return;
    }

    /* check for the tracker and create it if not found */
    if (NULL == (coll = prte_grpcomm_base_get_tracker(sig, true))) {
        PRTE_ERROR_LOG(PRTE_ERR_NOT_FOUND);
        PRTE_RELEASE(sig);
        return;
    }

    /* unpack the mode */
    cnt = 1;
    if (PRTE_SUCCESS != (rc = prte_dss.unpack(buffer, &mode, &cnt, PRTE_INT))) {
        PRTE_ERROR_LOG(rc);
        return;
    }
    /* increment nprocs reported for collective */
    coll->nreported++;
    /* capture any provided content */
    prte_dss.copy_payload(&coll->bucket, buffer);

    PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx allgather recv nexpected %d nrep %d",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME),
                         (int)coll->nexpected, (int)coll->nreported));

    /* see if everyone has reported */
    if (coll->nreported == coll->nexpected) {
        if (PRTE_PROC_IS_MASTER) {
            PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:ucx allgather HNP reports complete",
                                 PRTE_NAME_PRINT(PRTE_PROC_MY_NAME)));
            /* the allgather is complete - send the xcast */
            reply = PRTE_NEW(prte_buffer_t);
            /* pack the signature */
            if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &sig, 1, PRTE_SIGNATURE))) {
                PRTE_ERROR_LOG(rc);
                PRTE_RELEASE(reply);
                PRTE_RELEASE(sig);
                return;
            }
            /* pack the status - success since the allgather completed. This
             * would be an error if we timeout instead */
            ret = PRTE_SUCCESS;
            if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &ret, 1, PRTE_INT))) {
                PRTE_ERROR_LOG(rc);
                PRTE_RELEASE(reply);
                PRTE_RELEASE(sig);
                return;
            }
            /* pack the mode */
            if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &mode, 1, PRTE_INT))) {
                PRTE_ERROR_LOG(rc);
                PRTE_RELEASE(reply);
                PRTE_RELEASE(sig);
                return;
            }
            /* if we were asked to provide a context id, do so */
            if (1 == mode) {
                size_t sz;
                sz = prte_grpcomm_base.context_id;
                ++prte_grpcomm_base.context_id;
                if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &sz, 1, PRTE_SIZE))) {
                    PRTE_ERROR_LOG(rc);
                    PRTE_RELEASE(reply);
                    PRTE_RELEASE(sig);
                    return;
                }
            }
            /* transfer the collected bucket */
            prte_dss.copy_payload(reply, &coll->bucket);
            /* send the release via xcast */
            (void)prte_grpcomm.xcast(sig, PRTE_RML_TAG_COLL_RELEASE, reply);
            PRTE_RELEASE(reply);
        } else {
            PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:ucx allgather rollup complete - sending to %s",
                                 PRTE_NAME_PRINT(PRTE_PROC_MY_NAME),
                                 PRTE_NAME_PRINT(PRTE_PROC_MY_PARENT)));
            /* relay the bucket upward */
            reply = PRTE_NEW(prte_buffer_t);
            /* pack the signature */
            if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &sig, 1, PRTE_SIGNATURE))) {
                PRTE_ERROR_LOG(rc);
                PRTE_RELEASE(reply);
                PRTE_RELEASE(sig);
                return;
            }
            /* pack the mode */
            if (PRTE_SUCCESS != (rc = prte_dss.pack(reply, &mode, 1, PRTE_INT))) {
                PRTE_ERROR_LOG(rc);
                PRTE_RELEASE(reply);
                PRTE_RELEASE(sig);
                return;
            }
            /* transfer the collected bucket */
            prte_dss.copy_payload(reply, &coll->bucket);
            /* send the info to our parent */
            rc = prte_rml.send_buffer_nb(PRTE_PROC_MY_PARENT, reply,
                                         PRTE_RML_TAG_ALLGATHER_UCX,
                                         prte_rml_send_callback, NULL);
        }
    }
    PRTE_RELEASE(sig);
}

static void xcast_recv(int status, prte_process_name_t* sender,
                       prte_buffer_t* buffer, prte_rml_tag_t tg,
                       void* cbdata)
{
    prte_list_item_t *item;
    prte_namelist_t *nm;
    int ret, cnt;
    prte_buffer_t *relay=NULL, *rly;
    prte_daemon_cmd_flag_t command = PRTE_DAEMON_NULL_CMD;
    prte_buffer_t datbuf, *data;
    int8_t flag;
    prte_job_t *jdata;
    prte_proc_t *rec;
    prte_list_t coll;
    prte_grpcomm_signature_t *sig;
    prte_rml_tag_t tag;
    size_t inlen, cmplen;
    uint8_t *packed_data, *cmpdata;
    ucs_status_t st;

    prte_output(0, "grpcomm_ucx ==> %s entry, tag=%u\n", __func__, tg);

    PRTE_OUTPUT_VERBOSE((1, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx:xcast:recv: with %d bytes",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME),
                         (int)buffer->bytes_used));

    /* we need a passthru buffer to send to our children - we leave it
     * as compressed data */
    rly = PRTE_NEW(prte_buffer_t);
    prte_dss.copy_payload(rly, buffer);
    PRTE_CONSTRUCT(&datbuf, prte_buffer_t);
    /* setup the relay list */
    PRTE_CONSTRUCT(&coll, prte_list_t);

    /* unpack the flag to see if this payload is compressed */
    cnt=1;
    if (PRTE_SUCCESS != (ret = prte_dss.unpack(buffer, &flag, &cnt, PRTE_INT8))) {
        PRTE_ERROR_LOG(ret);
        PRTE_FORCED_TERMINATE(ret);
        PRTE_DESTRUCT(&datbuf);
        PRTE_DESTRUCT(&coll);
        PRTE_RELEASE(rly);
        return;
    }
    if (flag) {
        /* unpack the data size */
        cnt=1;
        if (PRTE_SUCCESS != (ret = prte_dss.unpack(buffer, &inlen, &cnt, PRTE_SIZE))) {
            PRTE_ERROR_LOG(ret);
            PRTE_FORCED_TERMINATE(ret);
            PRTE_DESTRUCT(&datbuf);
            PRTE_DESTRUCT(&coll);
            PRTE_RELEASE(rly);
            return;
        }
        /* unpack the unpacked data size */
        cnt=1;
        if (PRTE_SUCCESS != (ret = prte_dss.unpack(buffer, &cmplen, &cnt, PRTE_SIZE))) {
            PRTE_ERROR_LOG(ret);
            PRTE_FORCED_TERMINATE(ret);
            PRTE_DESTRUCT(&datbuf);
            PRTE_DESTRUCT(&coll);
            PRTE_RELEASE(rly);
            return;
        }
        /* allocate the space */
        packed_data = (uint8_t*)malloc(inlen);
        /* unpack the data blob */
        cnt = inlen;
        if (PRTE_SUCCESS != (ret = prte_dss.unpack(buffer, packed_data, &cnt, PRTE_UINT8))) {
            PRTE_ERROR_LOG(ret);
            free(packed_data);
            PRTE_FORCED_TERMINATE(ret);
            PRTE_DESTRUCT(&datbuf);
            PRTE_DESTRUCT(&coll);
            PRTE_RELEASE(rly);
            return;
        }
        /* decompress the data */
        if (prte_compress.decompress_block(&cmpdata, cmplen,
                                       packed_data, inlen)) {
            /* the data has been uncompressed */
            prte_dss.load(&datbuf, cmpdata, cmplen);
            data = &datbuf;
        } else {
            data = buffer;
        }
        free(packed_data);
    } else {
        data = buffer;
    }

    /* get the signature that we do not need */
    cnt=1;
    if (PRTE_SUCCESS != (ret = prte_dss.unpack(data, &sig, &cnt, PRTE_SIGNATURE))) {
        PRTE_ERROR_LOG(ret);
        PRTE_DESTRUCT(&datbuf);
        PRTE_DESTRUCT(&coll);
        PRTE_RELEASE(rly);
        PRTE_FORCED_TERMINATE(ret);
        return;
    }
    PRTE_RELEASE(sig);

    /* get the target tag */
    cnt=1;
    if (PRTE_SUCCESS != (ret = prte_dss.unpack(data, &tag, &cnt, PRTE_RML_TAG))) {
        PRTE_ERROR_LOG(ret);
        PRTE_DESTRUCT(&datbuf);
        PRTE_DESTRUCT(&coll);
        PRTE_RELEASE(rly);
        PRTE_FORCED_TERMINATE(ret);
        return;
    }

    /* copy the msg for relay to ourselves */
    relay = PRTE_NEW(prte_buffer_t);
    prte_dss.copy_payload(relay, data);

    if (!prte_do_not_launch) {
        /* get the list of next recipients from the routed module */
        prte_routed.get_routing_list(&coll);

        /* if list is empty, no relay is required */
        if (prte_list_is_empty(&coll)) {
            PRTE_OUTPUT_VERBOSE((5, prte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:ucx:send_relay - recipient list is empty!",
                                 PRTE_NAME_PRINT(PRTE_PROC_MY_NAME)));
            goto CLEANUP;
        }

        /* send the message to each recipient on list, deconstructing it as we go */
        while (NULL != (item = prte_list_remove_first(&coll))) {
            nm = (prte_namelist_t*)item;

            PRTE_OUTPUT_VERBOSE((5, prte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:ucx:send_relay sending relay msg of %d bytes to %s",
                                 PRTE_NAME_PRINT(PRTE_PROC_MY_NAME), (int)rly->bytes_used,
                                 PRTE_NAME_PRINT(&nm->name)));
            PRTE_RETAIN(rly);
            /* check the state of the recipient - no point
             * sending to someone not alive
             */
            jdata = prte_get_job_data_object(nm->name.jobid);
            if (NULL == (rec = (prte_proc_t*)prte_pointer_array_get_item(jdata->procs, nm->name.vpid))) {
                if (!prte_abnormal_term_ordered && !prte_prteds_term_ordered) {
                    prte_output(0, "%s grpcomm:ucx:send_relay proc %s not found - cannot relay",
                                PRTE_NAME_PRINT(PRTE_PROC_MY_NAME), PRTE_NAME_PRINT(&nm->name));
                }
                PRTE_RELEASE(rly);
                PRTE_RELEASE(item);
                PRTE_FORCED_TERMINATE(PRTE_ERR_UNREACH);
                continue;
            }
            if ((PRTE_PROC_STATE_RUNNING < rec->state &&
                PRTE_PROC_STATE_CALLED_ABORT != rec->state) ||
                !PRTE_FLAG_TEST(rec, PRTE_PROC_FLAG_ALIVE)) {
                if (!prte_abnormal_term_ordered && !prte_prteds_term_ordered) {
                    prte_output(0, "%s grpcomm:ucx:send_relay proc %s not running - cannot relay: %s ",
                                PRTE_NAME_PRINT(PRTE_PROC_MY_NAME), PRTE_NAME_PRINT(&nm->name),
                                PRTE_FLAG_TEST(rec, PRTE_PROC_FLAG_ALIVE) ? prte_proc_state_to_str(rec->state) : "NOT ALIVE");
                }
                PRTE_RELEASE(rly);
                PRTE_RELEASE(item);
                PRTE_FORCED_TERMINATE(PRTE_ERR_UNREACH);
                continue;
            }

            if (grpcomm_ucx_bcast_handshake_success) {
                /* Prepare send buffer for broadcast (we always broadcast the complete buffer size) */
                assert(rly->bytes_used > GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE);
                memcpy(grpcomm_xcast_send_buffer, rly->base_ptr, rly->bytes_used);
                *(size_t *)(&grpcomm_xcast_send_buffer[XCAST_BUFFER_SEND_SIZE_OFFSET]) = rly->bytes_used;

                prte_output(0, "ucx ==> xcast_recv() Preparing Bcast: bytes_used=%zu\n", rly->bytes_used);

                st = ucg_minimal_broadcast(&g_ucg_context, grpcomm_xcast_send_buffer, GRPCOMM_UCX_XCAST_RECV_BUFFER_SIZE);
                if (st != UCS_OK) {
                    prte_output(10, "xcast_recv(): ucg_minimal_broadcast() failed, status=%d\n", st);

                    PRTE_ERROR_LOG(ret);
                    PRTE_RELEASE(rly);
                    PRTE_RELEASE(item);
                    PRTE_FORCED_TERMINATE(PRTE_ERR_UNREACH);
                    continue;
                }
            }
            else {
                if (PRTE_SUCCESS != (ret = prte_rml.send_buffer_nb(&nm->name, rly, PRTE_RML_TAG_XCAST,
                                                                   prte_rml_send_callback, NULL))) {
                    PRTE_ERROR_LOG(ret);
                    PRTE_RELEASE(rly);
                    PRTE_RELEASE(item);
                    PRTE_FORCED_TERMINATE(PRTE_ERR_UNREACH);
                    continue;
                }
            }
            PRTE_RELEASE(item);
        }
    }

 CLEANUP:
    /* cleanup */
    PRTE_LIST_DESTRUCT(&coll);
    PRTE_RELEASE(rly);  // retain accounting

    /* now pass the relay buffer to myself for processing - don't
     * inject it into the RML system via send as that will compete
     * with the relay messages down in the OOB. Instead, pass it
     * ucxly to the RML message processor */
    if (PRTE_DAEMON_DVM_NIDMAP_CMD != command) {
        PRTE_RML_POST_MESSAGE(PRTE_PROC_MY_NAME, tag, 1,
                              relay->base_ptr, relay->bytes_used);
        relay->base_ptr = NULL;
        relay->bytes_used = 0;
    }
    if (NULL != relay) {
        PRTE_RELEASE(relay);
    }
    PRTE_DESTRUCT(&datbuf);
}

static void barrier_release(int status, prte_process_name_t* sender,
                            prte_buffer_t* buffer, prte_rml_tag_t tag,
                            void* cbdata)
{
    int32_t cnt;
    int rc, ret, mode;
    prte_grpcomm_signature_t *sig;
    prte_grpcomm_coll_t *coll;

    prte_output(0, "grpcomm_ucx ==> %s entry, tag=%u\n", __func__, tag);

    PRTE_OUTPUT_VERBOSE((5, prte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:ucx: barrier release called with %d bytes",
                         PRTE_NAME_PRINT(PRTE_PROC_MY_NAME), (int)buffer->bytes_used));

    /* unpack the signature */
    cnt = 1;
    if (PRTE_SUCCESS != (rc = prte_dss.unpack(buffer, &sig, &cnt, PRTE_SIGNATURE))) {
        PRTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the return status */
    cnt = 1;
    if (PRTE_SUCCESS != (rc = prte_dss.unpack(buffer, &ret, &cnt, PRTE_INT))) {
        PRTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the mode */
    cnt = 1;
    if (PRTE_SUCCESS != (rc = prte_dss.unpack(buffer, &mode, &cnt, PRTE_INT))) {
        PRTE_ERROR_LOG(rc);
        return;
    }

    /* check for the tracker - it is not an error if not
     * found as that just means we wre not involved
     * in the collective */
    if (NULL == (coll = prte_grpcomm_base_get_tracker(sig, false))) {
        PRTE_RELEASE(sig);
        return;
    }

    /* execute the callback */
    if (NULL != coll->cbfunc) {
        coll->cbfunc(ret, buffer, coll->cbdata);
    }
    prte_list_remove_item(&prte_grpcomm_base.ongoing, &coll->super);
    PRTE_RELEASE(coll);
    PRTE_RELEASE(sig);
}
