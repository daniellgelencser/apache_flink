import time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.functions import MapFunction, ReduceFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class Aggregation(ReduceFunction):

    def open(self, runtime_context:RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)

    def reduce(self, a, b):
        a = (0, 0) if a is None else a
        b = (0, 0) if b is None else b
        return a[0] + b[0], max(a[1], b[1])


class Wrapper(MapFunction):

    def open(self, runtime_context:RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, aggregated_result):
        end_time = time.time()

        return {
            'gemPackID': aggregated_result[0],
            'price': aggregated_result[1][0],
            'time': end_time,
            'latency': end_time - aggregated_result[1][1]
        }

if __name__ == '__main__':
    environment = StreamExecutionEnvironment.get_execution_environment()

    data_stream = DataStream(environment._j_stream_execution_environment.socketTextStream('localhost', 9999))
    # data_stream.print()


    data_stream = data_stream.key_by(lambda purchase: (str(purchase['gemPackID']), (purchase['price'], purchase['time']))) \
            .reduce(Aggregation()) \
            .map(Wrapper()) 

    environment.execute('socket_stream')
