/* -*- C -*-
 *
 * Copyright (c) 2011-2020 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef _GRPCOMM_UCX_H_
#define _GRPCOMM_UCX_H_

#include "prte_config.h"


#include "src/mca/grpcomm/grpcomm.h"

BEGIN_C_DECLS

/*
 * Grpcomm interfaces
 */

PRTE_MODULE_EXPORT extern prte_grpcomm_base_component_t prte_grpcomm_ucx_component;
extern prte_grpcomm_base_module_t prte_grpcomm_ucx_module;

END_C_DECLS

#endif
