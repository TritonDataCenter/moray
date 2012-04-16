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
SMF_MANIFESTS_IN = smf/manifests/moray.xml.in

CLEAN_FILES	+= node_modules $(SHRINKWRAP) cscope.files


include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node.defs
include ./tools/mk/Makefile.smf.defs

#
# Variables
#

# Mountain Gorilla-spec'd versioning.
# Need GNU awk for multi-char arg to "-F".
AWK := $(shell (which gawk 2>/dev/null | grep -v "^no ") || (which nawk 2>/dev/null | grep -v "^no ") || which awk)
BRANCH := $(shell git symbolic-ref HEAD | $(AWK) -F/ '{print $$3}')
ifeq ($(TIMESTAMP),)
	TIMESTAMP := $(shell date -u "+%Y%m%dT%H%M%SZ")
endif
GITDESCRIBE := g$(shell git describe --all --long --dirty | $(AWK) -F'-g' '{print $$NF}')
STAMP := $(BRANCH)-$(TIMESTAMP)-$(GITDESCRIBE)

ROOT                    := $(shell pwd)
RELEASE_TARBALL         := moray-pkg-$(STAMP).tar.bz2
TMPDIR                  := /tmp/$(STAMP)


#
# Env vars
#
PATH	:= $(NODE_INSTALL)/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(NODEUNIT) $(REPO_DEPS)
	$(NPM) install

$(NODEUNIT): | $(NPM_EXEC)
	$(NPM) install

.PHONY: shrinkwrap
shrinkwrap: | $(NPM_EXEC)
	$(NPM) shrinkwrap

.PHONY: test
test: $(NODEUNIT)
	$(NODEUNIT) test/buckets.db.test.js --reporter tap
	$(NODEUNIT) test/objects.db.test.js --reporter tap
	$(NODEUNIT) test/buckets.test.js --reporter tap
	$(NODEUNIT) test/objects.test.js --reporter tap

.PHONY: cover
cover: $(NODECOVER)
	@rm -fr ./.coverage_data
	@MORAY_COVERAGE=1 LOG_LEVEL=error $(NODECOVER) run $(NODEUNIT) test/buckets.db.test.js
	@MORAY_COVERAGE=1 LOG_LEVEL=error $(NODECOVER) run $(NODEUNIT) test/objects.db.test.js
	@MORAY_COVERAGE=1 LOG_LEVEL=error $(NODECOVER) run $(NODEUNIT) test/buckets.test.js
	@MORAY_COVERAGE=1 LOG_LEVEL=error $(NODECOVER) run $(NODEUNIT) test/buckets.test.js
	$(NODECOVER) report html

.PHONY: pkg
pkg: all

.PHONY: release
release: pkg
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(TMPDIR)/root/opt/smartdc/moray
	@mkdir -p $(TMPDIR)/site
	@touch $(TMPDIR)/site/.do-not-delete-me
	@mkdir -p $(TMPDIR)/root
	@mkdir -p $(TMPDIR)/root/opt/smartdc/moray/ssl
	cp -r   $(ROOT)/build \
		$(ROOT)/lib \
		$(ROOT)/main.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/smf \
		$(TMPDIR)/root/opt/smartdc/moray/
	(cd $(TMPDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root site)
	@rm -rf $(TMPDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/moray
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/moray/$(RELEASE_TARBALL)


include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
