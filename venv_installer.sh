# This script installs all requirements in a virtual environment
# Virtual environment is located at ~/.virtualenvs, the default
# location for virtualenvwrapper.
#
# If the --startup flag is included, it will start up fes

#create virtualenv location
DIR="$HOME/.virtualenvs"
if [ ! -d "$DIR" ]; then
     mkdir $HOME/.virtualenvs
fi
cd $HOME/.virtualenvs

#create and activate fes virtualenv
virtualenv fes
source $HOME/.virtualenvs/fes/bin/activate

#install fes requirements
cd $HOME/fes
pip install -r requirements.txt

#optionally start up fes
if [ "$1" = "--startup" ]; then
    python $HOME/fes/src/rest_view.py &
fi
