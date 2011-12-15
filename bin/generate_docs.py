#!/usr/bin/env python

import os

SENSEI_HOME = os.path.normpath(os.path.join(os.path.normpath(__file__), '../..'))

# print SENSEI_HOME

os.chdir(SENSEI_HOME + "/docs")
os.system("mvn docbkx:generate-html")
os.system("cp -frp src/docbkx/figures target/docbkx/")
os.system("tar czf target/docbkx.tgz target/docbkx")

print "File docbkx.tgz is now available at %s/docs/target/docbkx.tgz" % SENSEI_HOME
