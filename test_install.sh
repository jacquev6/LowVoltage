#!/bin/bash

set -e

pip2 install virtualenv

dir=virtualenv_setup_install
rm -rf $dir
virtualenv $dir
. $dir/bin/activate
python2 setup.py install
cd $dir
python -c "import LowVoltage"
python -m LowVoltage.tests
cd ..
deactivate
rm -rf $dir
echo "Installing with 'python setup.py install' is OK"


echo
echo
dir=virtualenv_pip_install
rm -rf $dir
virtualenv $dir
cd $dir
. ./bin/activate
pip2 install LowVoltage
python -c "import LowVoltage"
python -m LowVoltage.tests
deactivate
cd ..
rm -rf $dir
echo "Installing with 'pip install LowVoltage' is OK"
