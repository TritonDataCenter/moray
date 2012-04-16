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
# Env vars
#
PATH	:= $(NODE_INSTALL)/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: tools $(SMF_MANIFESTS)


.PHONY: tools
tools: $(BUNYAN) $(JSONTOOL) $(NODEUNIT)

$(NODEUNIT): node_modules

.PHONY: node_modules
node_modules: | $(NPM_EXEC)
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

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
