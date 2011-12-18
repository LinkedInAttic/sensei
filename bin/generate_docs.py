#!/usr/bin/env python

import os

SENSEI_HOME = os.path.abspath(os.path.join(os.path.normpath(__file__), '../..'))

# print SENSEI_HOME

os.chdir(SENSEI_HOME + "/docs/src/docbkx/figures/BQL")
os.system("wish BQL-diagrams.tcl")

os.chdir(SENSEI_HOME + "/docs")
os.system("mvn docbkx:generate-html")
os.system("cp -frp src/docbkx/figures target/docbkx/")

# Remove unneeded files in figures directory
os.system("rm -fr target/docbkx/figures/*.graffle")
os.system("rm -fr target/docbkx/figures/BQL/.gitignore")
os.system("rm -fr target/docbkx/figures/BQL/*.pdf")
os.system("rm -fr target/docbkx/figures/BQL/*.ps")
os.system("rm -fr target/docbkx/figures/BQL/*.tcl")
os.system("rm -fr target/docbkx/figures/BQL/*.html")

os.chdir(SENSEI_HOME + "/docs/target")
os.system("tar czf docbkx.tgz docbkx")

print "File docbkx.tgz is now available at %s/docs/target/docbkx.tgz" % SENSEI_HOME
