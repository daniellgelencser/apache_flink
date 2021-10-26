echo "Get Python 3.8.12"
wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz
tar -xzf Python-3.8.12.tgz
rm Python-3.8.12.tgz

cd Python-3.8.12

echo "Install Python"
./configure --enable-optimizations
make

echo "Get pip"
wget https://bootstrap.pypa.io/get-pip.py

echo "Install pip"
./python get-pip.py
rm get-pip.py

echo "Install virtualenv"
./python -m pip install virtualenv==20.9.0

cd ..

echo "Create virtual environment"
Python-3.8.12/python -m virtualenv venv

echo "Activate virtual environment"
source venv/bin/activate

echo "Install Apache Flink 1.14.0"
pip install apache-flink==1.14.0