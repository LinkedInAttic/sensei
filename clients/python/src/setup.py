#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup
 
setup(
    name='sensei-python-client',
    version='1.0',
    description='This library implements a Sensei client',
    author='senseidb.com',
    url='https://github.com/javasoze/sensei',
    package_dir={'': '.'},
    py_modules=[
        'sensei',
    ],
)

