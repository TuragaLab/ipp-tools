""" Setup script
"""

from distutils.core import setup

setup(name='ipp-tools',
      version='0.01',
      description='IPyParallel utilities',
      author='Andrew Berger',
      author_email='bergera@janelia.hhmi.org',
      url='https://github.com/TuragaLab/ipp-tools',
      py_modules=['ipp_tools'],
      install_requires=['ipyparallel'])
