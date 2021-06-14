/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2020 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "prte_config.h"
#include "constants.h"

#include "src/mca/mca.h"
#include "src/runtime/prte_globals.h"
#include "src/mca/base/prte_mca_base_var.h"

#include "src/util/proc_info.h"

#include "grpcomm_ucx.h"

static int my_priority=5;  /* must be below "bad" module */
static int ucx_open(void);
static int ucx_close(void);
static int ucx_query(prte_mca_base_module_t **module, int *priority);
static int ucx_register(void);

/*
 * Struct of function pointers that need to be initialized
 */
prte_grpcomm_base_component_t prte_grpcomm_ucx_component = {
    .base_version = {
        PRTE_GRPCOMM_BASE_VERSION_3_0_0,

        .mca_component_name = "ucx",
        PRTE_MCA_BASE_MAKE_VERSION(component, PRTE_MAJOR_VERSION, PRTE_MINOR_VERSION,
                                    PRTE_RELEASE_VERSION),
        .mca_open_component = ucx_open,
        .mca_close_component = ucx_close,
        .mca_query_component = ucx_query,
        .mca_register_component_params = ucx_register,
    },
    .base_data = {
        /* The component is checkpoint ready */
        PRTE_MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int ucx_register(void)
{
    prte_mca_base_component_t *c = &prte_grpcomm_ucx_component.base_version;

    /* make the priority adjustable so users can select
     * ucx for use by apps without affecting daemons
     */
    my_priority = 85;
    (void) prte_mca_base_component_var_register(c, "priority",
                                           "Priority of the grpcomm ucx component",
                                           PRTE_MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           PRTE_MCA_BASE_VAR_FLAG_NONE,
                                           PRTE_INFO_LVL_9,
                                           PRTE_MCA_BASE_VAR_SCOPE_READONLY,
                                           &my_priority);
    return PRTE_SUCCESS;
}

/* Open the component */
static int ucx_open(void)
{
    return PRTE_SUCCESS;
}

static int ucx_close(void)
{
    return PRTE_SUCCESS;
}

static int ucx_query(prte_mca_base_module_t **module, int *priority)
{
    /* we are always available */
    *priority = my_priority;
    *module = (prte_mca_base_module_t *)&prte_grpcomm_ucx_module;
    return PRTE_SUCCESS;
}
