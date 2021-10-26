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

cd ../..
ln -s dep/Python-3.8.12/python py38

echo "Get pip"
wget https://bootstrap.pypa.io/get-pip.py

echo "Install pip"
./py38 get-pip.py
rm get-pip.py

echo "Install Apache Flink 1.14.0"
./py38 -m pip install apache-flink==1.14.0