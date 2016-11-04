# -*- coding: utf-8 -*-

from setuptools import setup

setup(name='bobos3',
      version='0.1.0',
      description='bobos3 is a simple library for accessing and manipulating objects via S3 API',
      url='http://github.com/contman/bobos3',
      author='CONTMAN sp. z o.o.',
      author_email='opensource@contman.pl',
      license='MIT',
      classifiers=[
          # How mature is this project? Common values are
          #   3 - Alpha
          #   4 - Beta
          #   5 - Production/Stable
          'Development Status :: 4 - Beta',

          # Indicate who your project is intended for
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Libraries :: Python Modules',

          # Pick your license as you wish (should match "license" above)
          'License :: OSI Approved :: MIT License',

          # Specify the Python versions you support here. In particular, ensure
          # that you indicate whether you support Python 2, Python 3 or both.
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
      ],
      packages=['bobos3'],
      install_requires=['requests', 'aws_requests_auth'],
      zip_safe=False
)