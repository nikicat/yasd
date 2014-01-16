#!/usr/bin/env python

from distutils.core import setup

setup(name='yasd',
      version='1.0',
      description='Yet Another Syslog Daemon',
      author='Nikolay Bryskin',
      author_email='devel.niks@gmail.com',
      url='https://github.com/nikicat/yasd',
      packages=['yasd'],
      requires=['cffi', 'pyelasticsearch'],
      scripts=['scripts/yasd']
     )
