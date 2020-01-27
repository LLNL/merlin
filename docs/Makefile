# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = Merlin
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile


check-permission:
	if id -nG $(USER) | grep -qw "merlin" ; then printf "You are in group: merlin\n" ; else printf "Permission error: must be in group 'merlin', for merlin developers\n" ; false ; fi


deploy: check-permission html
	rm -rf $(MERLIN_WWW_INSTALL_PATH)/*
	cp -a ./build/html/. $(MERLIN_WWW_INSTALL_PATH)
	chgrp -R merlin $(MERLIN_WWW_INSTALL_PATH)/*
	find $(MERLIN_WWW_INSTALL_PATH) -type d -exec chmod 775 {} \;
	find $(MERLIN_WWW_INSTALL_PATH) -type f -exec chmod 664 {} \;

code-docs:
	sphinx-apidoc -f -o source/ ../merlin/

view: code-docs html
	firefox -new-instance build/html/index.html

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	echo $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

