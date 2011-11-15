import sys

try:
  from setuptools import setup, find_packages
except ImportError:
  print 'Error: setuptools is required. http://pypi.python.org/pypi/setuptools'
  sys.exit(1)

setup(
  name          = 'sensei',
  version       = '1.0',
  description   = 'Sensei client library',
  author        = 'senseidb.com',
  url           = 'https://github.com/javasoze/sensei',
  packages      = find_packages(),
  classifiers   = ['Development Status :: 5 - Production/Stable',
                   'Intended Audience :: Developers',
                   'License :: APL',
                   'Operating System :: OS Independent',
                   'Topic :: Internet'],
)
