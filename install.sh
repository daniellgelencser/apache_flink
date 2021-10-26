mkdir dep
cd dep

echo "Get Python 3.8.12"
wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz
tar -xzf Python-3.8.12.tgz
rm Python-3.8.12.tgz

echo "Install Python"
cd Python-3.8.12
./configure --enable-optimizations
make

echo "Get pip"
wget https://bootstrap.pypa.io/get-pip.py

echo "Install pip"
./python get-pip.py
rm get-pip.py

echo "Install depencencies"
Python-3.8.12/python -m pip install -r ../../dependencies.txt