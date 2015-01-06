# This script installs all requirements in a virtual environment
# Virtual environment is located at ~/.virtualenvs, the default
# location for virtualenvwrapper.

#create virtualenv location
mkdir $HOME/.virtualenvs
cd $HOME/.virtualenvs

#create and activate fes virtualenv
virtualenv fes
source $HOME/.virtualenvs/fes/bin/activate

#install fes requirements
cd $HOME/fes
pip install -r requirements.txt
