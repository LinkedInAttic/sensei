#
# File: Sensei/Docs/Makefile
#
# $Id$
#

DOCBOOK_XSL_NS=~/Downloads/docbook-xsl-ns
DOCBOOK4YST=~/Knowledge/DocBook/docbook4yst
FOP=~/Downloads/fop-1.0/fop
XMLFILE=sensei

# /Users/bcui/Downloads/docbook-xsl-ns/extensions/saxon65.jar is
# required to process callouts.

export CLASSPATH=.:/Users/bcui/Downloads/saxon6-5-5/saxon.jar:/Users/bcui/Downloads/docbook-xsl-ns/extensions/saxon65.jar:/Users/bcui/Downloads/xslthl-2-1/xslthl-2.0.1.jar:/Users/bcui/Downloads/fop-pdf-images-2.0.0.SNAPSHOT/fop-pdf-images-2.0.0.SNAPSHOT.jar:/Users/bcui/Downloads/fop-pdf-images-2.0.0.SNAPSHOT/pdfbox-0.8.0-incubating.jar
HEAP_OPTS=-Xmx1g -Xms1g

.PHONY: all pdf html html-chunk twiki txt install

all: pdf html html-chunk wiki

pdf:
	cd src/docbkx/; java $(HEAP_OPTS) com.icl.saxon.StyleSheet	\
	     -o $(XMLFILE).fo				\
	     $(XMLFILE).xml				\
	     $(DOCBOOK4YST)/yst-docbook.xsl

#	~/XEP/xep -fo $(XMLFILE).fo $(XMLFILE).pdf -pdf
	cd src/docbkx/; $(FOP) $(XMLFILE).fo $(XMLFILE).pdf

html:
	java com.icl.saxon.StyleSheet					\
	     -o $(XMLFILE).html						\
	     $(XMLFILE).xml						\
	     $(DOCBOOK4YST)/yst-docbook-html.xsl			\
	     chunker.output.indent="yes"				\
	     use.extensions=1						\
	     graphicsize.extension=1					\
	     section.autolabel=1					\
	     admon.graphics=1						\
	     admon.graphics.path="docbook-xsl-ns/images/"		\
	     admon.graphics.extension=".png"				\
	     callout.graphics=1						\
	     callout.graphics.path="docbook-xsl-ns/images/callouts/"	\
	     callout.graphics.extension=".png"				\
	     title.margin.left="0pt"					\
	     ulink.show=0						\
	     tablecolumns.extension=0

	mkdir -p html/figures
	mkdir -p html/docbook-xsl-ns
	mv $(XMLFILE).html html
	cp figures/*.png html/figures
#	Copy images used by admon and callout.
	cp -frp $(DOCBOOK_XSL_NS)/images html/docbook-xsl-ns

html-chunk:
#	xsltproc may display smaller PNG files than what Saxon does.  In
#	order to be consistent, we use Saxon to generate both single
#	page HTML and chunk HTML pages.
#
# 	xsltproc --xinclude				 \
# 	         --stringparam chunker.output.indent yes \
# 		 $(DOCBOOK_XSL_NS)/xhtml/chunk.xsl \
# 		 $(XMLFILE).xml
#
	java com.icl.saxon.StyleSheet					\
	     $(XMLFILE).xml						\
	     $(DOCBOOK4YST)/yst-docbook-html-chunk.xsl			\
	     chunker.output.indent="yes"				\
	     use.extensions=1						\
	     graphicsize.extension=1					\
	     section.autolabel=1					\
	     admon.graphics=1						\
	     admon.graphics.path="docbook-xsl-ns/images/"		\
	     admon.graphics.extension=".png"				\
	     callout.graphics=1						\
	     callout.graphics.path="docbook-xsl-ns/images/callouts/"	\
	     callout.graphics.extension=".png"				\
	     title.margin.left="0pt"					\
	     ulink.show=0						\
	     tablecolumns.extension=0

	mkdir -p html/figures
	mkdir -p html/docbook-xsl-ns
	mv *.html html
	cp figures/*.png html/figures
#	Copy images used by admon and callout.
	cp -frp $(DOCBOOK_XSL_NS)/images html/docbook-xsl-ns

wiki:
	xsltproc --output $(XMLFILE).wiki			\
	         /Users/bcui/docbook2twiki/docbook2cfl.xsl	\
		 $(XMLFILE).xml

txt: html
	lynx -dump -nolist html/$(XMLFILE).html | sed "s/C\&#8288;\+\&#8288;\+/C\+\+/" > $(XMLFILE).txt

