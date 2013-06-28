#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
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
# Tools
#
NODE		:= ./build/node/bin/node
NODEUNIT	:= ./node_modules/.bin/nodeunit
NODECOVER	:= ./node_modules/.bin/cover
BUNYAN		:= ./node_modules/.bin/bunyan
JSONTOOL	:= ./node_modules/.bin/json

#
# Files
#
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell ls *.js) $(shell find lib test -name '*.js' | grep -v sql.js)
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -C -f ./tools/jsstyle.conf
SHRINKWRAP	 = npm-shrinkwrap.json
SMF_MANIFESTS_IN = smf/manifests/haproxy.xml.in

CLEAN_FILES	+= node_modules $(SHRINKWRAP) cscope.files

#
# Variables
#

NODE_PREBUILT_TAG	= zone
NODE_PREBUILT_VERSION	:= v0.10.12

# RELENG-341: no npm cache is making builds unreliable
NPM_FLAGS :=

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.node_deps.defs
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL         := moray-pkg-$(STAMP).tar.bz2
ROOT                    := $(shell pwd)
TMPDIR                  := /tmp/$(STAMP)


#
# Env vars
#
PATH	:= $(NODE_INSTALL)/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) deps scripts

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM_ENV) $(NPM) install

.PHONY: shrinkwrap
shrinkwrap: | $(NPM_EXEC)
	$(NPM) shrinkwrap

.PHONY: test
test: $(NODEUNIT)
	$(NODEUNIT) test/buckets.test.js | $(BUNYAN)
	$(NODEUNIT) test/objects.test.js | $(BUNYAN)
	$(NODEUNIT) test/integ.test.js | $(BUNYAN)


.PHONY: cover
cover: $(NODECOVER)
	@rm -fr ./.coverage_data
	LOG_LEVEL=error $(NODECOVER) run main.js -- -f ./etc/config.laptop.json -c -s &
	@sleep 3
	$(NODEUNIT) test/buckets.test.js
	$(NODEUNIT) test/objects.test.js
	@pkill -17 node
	@sleep 3
	$(NODECOVER) report

.PHONY: release
release: all docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(TMPDIR)/root/opt/smartdc/moray
	@mkdir -p $(TMPDIR)/root/opt/smartdc/boot
	@mkdir -p $(TMPDIR)/root/opt/smartdc/moray/etc
	cp -r   $(ROOT)/bin \
		$(ROOT)/boot\
		$(ROOT)/build \
		$(ROOT)/lib \
		$(ROOT)/main.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/sapi_manifests \
		$(ROOT)/sdc \
		$(ROOT)/smf \
		$(TMPDIR)/root/opt/smartdc/moray/
	cp $(ROOT)/etc/config.json.in $(TMPDIR)/root/opt/smartdc/moray/etc
	cp $(ROOT)/etc/haproxy.cfg.in $(TMPDIR)/root/opt/smartdc/moray/etc
	mv $(TMPDIR)/root/opt/smartdc/moray/build/scripts \
	    $(TMPDIR)/root/opt/smartdc/moray/boot
	ln -s /opt/smartdc/moray/boot/configure.sh \
	    $(TMPDIR)/root/opt/smartdc/boot/configure.sh
	chmod 755 $(TMPDIR)/root/opt/smartdc/moray/boot/configure.sh
	(cd $(TMPDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root)
	@rm -rf $(TMPDIR)


.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/moray
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/moray/$(RELEASE_TARBALL)


include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
