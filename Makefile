#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
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
SMF_MANIFESTS_IN = smf/manifests/haproxy.xml.in
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
	NODE_PREBUILT_VERSION	:= v4.9.0
endif

# RELENG-341: no npm cache is making builds unreliable
NPM_FLAGS :=

include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	NODE := node
	NPM := $(shell which npm)
	NPM_EXEC=$(NPM)
endif
include ./tools/mk/Makefile.node_deps.defs
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL         := moray-pkg-$(STAMP).tar.bz2
ROOT                    := $(shell pwd)
RELSTAGEDIR                  := /tmp/$(STAMP)


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
	(cd $(RELSTAGEDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root)
	@rm -rf $(RELSTAGEDIR)


.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/moray
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/moray/$(RELEASE_TARBALL)

%/template: %/template.in
	sed -e 's/@@PORTS@@/2020/g' $< > $@

include ./tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.targ
else
	include ./tools/mk/Makefile.node.targ
endif
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ

sdc-scripts: deps/sdc-scripts/.git
