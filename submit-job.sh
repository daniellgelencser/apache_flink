echo "Start aggregation job"

source venv/bin/activate     
cd flink-1.14.0
./bin/flink run -pyexec ../venv/bin/python -py ../flink.py
