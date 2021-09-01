#
# Copyright (c) 2011-2020 Cisco Systems, Inc.  All rights reserved
# Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
#                         reserved.
# Copyright (c) 2014-2020 Intel, Inc.  All rights reserved.
# Copyright (c) 2017      IBM Corporation.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

AM_CPPFLAGS = $(grpcomm_ucx_CPPFLAGS)

sources = \
	grpcomm_ucx.h \
	grpcomm_ucx.c \
	grpcomm_ucx_component.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

#if PRRTE_HAVE_UCX 

if MCA_BUILD_prte_grpcomm_ucx_DSO
component_noinst =
component_install = mca_grpcomm_ucx.la
else
component_noinst = libmca_grpcomm_ucx.la
component_install =
endif

#endif

mcacomponentdir = $(prtelibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_grpcomm_ucx_la_SOURCES = $(sources)
mca_grpcomm_ucx_la_LDFLAGS = -module -avoid-version -lucs -lucp -lucg -L/mnt/central/users/szanyovka/ucg-bcast/hucx/build/lib
mca_grpcomm_ucx_la_LIBADD = $(top_builddir)/src/libprrte.la


noinst_LTLIBRARIES = $(component_noinst)
libmca_grpcomm_ucx_la_SOURCES =$(sources)
libmca_grpcomm_ucx_la_LDFLAGS = -module -avoid-version

