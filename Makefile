#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2019, Joyent, Inc.
#

#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

NAME = moray

#
# Files
#
DOC_FILES	 = index.md
JS_FILES	:= $(shell ls *.js) $(shell find lib -name '*.js' | grep -v sql.js)
ESLINT_FILES	:= $(JS_FILES)
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -C -f ./tools/jsstyle.conf
SMF_MANIFESTS_IN =	smf/manifests/haproxy.xml.in
BOOTSTRAP_MANIFESTS = 	sapi_manifests/registrar/template \
			sdc/sapi_manifests/registrar/template

CLEAN_FILES	+= node_modules cscope.files \
		   $(BOOTSTRAP_MANIFESTS)

#
# Variables
#

ifeq ($(shell uname -s),SunOS)
	# Allow building on a SmartOS image other than sdc-*-multiarch 15.4.1.
	NODE_PREBUILT_IMAGE	= 18b094b0-eb01-11e5-80c1-175dac7ddf02
	NODE_PREBUILT_TAG	= zone
	NODE_PREBUILT_VERSION	:= v6.17.0
endif

# RELENG-341: no npm cache is making builds unreliable
NPM_FLAGS :=

ENGBLD_USE_BUILDIMAGE	= true
ENGBLD_REQUIRE		:= $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
else
	NODE := node
	NPM := $(shell which npm)
	NPM_EXEC=$(NPM)
endif
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL         := $(NAME)-pkg-$(STAMP).tar.gz
ROOT                    := $(shell pwd)
RELSTAGEDIR                  := /tmp/$(NAME)-$(STAMP)

BASE_IMAGE_UUID = 04a48d7d-6bb5-4e83-8c3b-e60a99e0f48f
BUILDIMAGE_NAME = manta-moray
BUILDIMAGE_DESC	= Manta moray
BUILDIMAGE_PKGSRC = haproxy-1.6.2 postgresql92-client-9.2.19
AGENTS		= amon config registrar

#
# Env vars
#
PATH	:= $(NODE_INSTALL)/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) deps scripts sdc-scripts

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM) install --production

.PHONY: test
test:
	@echo See the separate moray-test-suite repository for testing.

.PHONY: scripts
scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts


.PHONY: release
release: all docs $(SMF_MANIFESTS) $(BOOTSTRAP_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/moray
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/moray/etc
	cp -r   $(ROOT)/bin \
		$(ROOT)/build \
		$(ROOT)/lib \
		$(ROOT)/main.js \
		$(ROOT)/pg-setup.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/sapi_manifests \
		$(ROOT)/sdc \
		$(ROOT)/smf \
		$(RELSTAGEDIR)/root/opt/smartdc/moray/
	cp $(ROOT)/etc/config.json.in $(RELSTAGEDIR)/root/opt/smartdc/moray/etc
	cp $(ROOT)/etc/haproxy.cfg.in $(RELSTAGEDIR)/root/opt/smartdc/moray/etc
	mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts
	cp -R $(RELSTAGEDIR)/root/opt/smartdc/moray/build/scripts/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts/
	cp -R $(ROOT)/deps/sdc-scripts/* $(RELSTAGEDIR)/root/opt/smartdc/boot/
	cp -R $(ROOT)/boot/* $(RELSTAGEDIR)/root/opt/smartdc/boot/
	(cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(ROOT)/$(RELEASE_TARBALL) root)
	@rm -rf $(RELSTAGEDIR)


.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/moray
	cp $(ROOT)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/moray/$(RELEASE_TARBALL)

%/template: %/template.in
	sed -e 's/@@PORTS@@/2020/g' $< > $@

include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
else
	include ./deps/eng/tools/mk/Makefile.node.targ
endif
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ

sdc-scripts: deps/sdc-scripts/.git
