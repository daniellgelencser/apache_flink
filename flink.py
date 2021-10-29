from datetime import datetime
import json, time, argparse
from typing import List, Tuple
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import BasicType, BasicTypeInfo, Types
from pyflink.common.types import Row
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.common.time import Instant

from pyflink.datastream import StreamExecutionEnvironment, DataStream, WindowAssigner, Trigger
from pyflink.datastream import data_stream
from pyflink.datastream import window
from pyflink.datastream.data_stream import ConnectedStreams, WindowedStream
from pyflink.datastream.functions import CoMapFunction, FilterFunction, MapFunction, ProcessWindowFunction, WindowFunction
from pyflink.datastream.window import TimeWindow, TimeWindowSerializer, TriggerResult
from pyflink.datastream.connectors import FileSink, OutputFileConfig

from pyflink.table import TableEnvironment
from pyflink.table import schema
from pyflink.table.environment_settings import EnvironmentSettings
from pyflink.table.schema import Schema
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.table.types import DataType, DataTypes

class TumblingWindowAssigner(WindowAssigner):

    def __init__(self, size, offset, is_event_time):
        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self, element, timestamp, context):
        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

    def get_default_trigger(self, env):
        return TimeTrigger()

    def get_window_serializer(self):
        return TimeWindowSerializer()

    def is_event_time(self):
        return self._is_event_time


class Aggregation(ProcessWindowFunction):

    def process(self, key, context, elements):
        price = 0
        event_time = 0
        processing_time = 0
        for i in elements:
            if(i is None):
                continue
            price += i[1]
            event_time = max(event_time, i[2]) 
            processing_time = max(processing_time, i[3]) 
        return [(key, price, event_time, processing_time)]

    def clear(self, context):
        pass

# class Join(CoMapFunction):

#     _values1: List[Tuple]
#     _values2: List[Tuple]

#     def map1(self, value):
#         for i in self._values2:
#             if i['user_id'] == value['user_id'] and i['gem_pack_id'] == value['gem_pack_id']:
#                 return 

#         self._values1.append(value)
#         self._map1_value = value
#         try: self._values2

#         except NameError: self._value2 = None

#     def map2(self, value):
#         return super().map2(value)


class TimeTrigger(Trigger):

    def on_element(self, element, timestamp, window, ctx):
        return TriggerResult.CONTINUE

    def on_processing_time(self, time, window, ctx):
        return TriggerResult.FIRE_AND_PURGE if time >= window.max_timestamp() else TriggerResult.CONTINUE

    def on_event_time(self, time, window, ctx):
        return TriggerResult.CONTINUE

    def on_merge(self, window, ctx):
        pass

    def clear(self, window, ctx):
        pass

class Wrapper(MapFunction):

    def map(self, aggregated_result):
        end_time = time.time()

        json_str = json.dumps({
            'gemPackID': aggregated_result[0],
            'price': aggregated_result[1],
            'end': end_time,
            'event time latency': end_time - aggregated_result[2],
            'processing time latency': end_time - aggregated_result[3]
        })
        print(json_str)

        return json_str


# class PurchaseInput(MapFunction):

#     def map(self, value):
        

class ProcessingTimeAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        return value[3]
        # return super().extract_timestamp(value, record_timestamp)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--mode', type=str, default="aggregation") # or join
    parser.add_argument('--api', type=str, default="stream") # or table
    args = parser.parse_args()

    print(str(args.mode))

    environment = StreamExecutionEnvironment \
        .get_execution_environment()

    data_stream = DataStream(environment._j_stream_execution_environment.socketTextStream('localhost', 9994))\
        .map(json.loads)

    # watermark_strategy = WatermarkStrategy \
    #     .for_monotonous_timestamps() \
    #     .with_timestamp_assigner(ProcessingTimeAssigner())

    # data_stream.assign_timestamps_and_watermarks(watermark_strategy=watermark_strategy)

    if(args.mode == "join"):

        settings = EnvironmentSettings \
            .new_instance() \
            .in_streaming_mode() \
            .use_blink_planner() \
            .build()

        table_env = StreamTableEnvironment \
            .create(stream_execution_environment=environment)

        purchase_stream = data_stream \
                .filter(lambda purchase: "price" in purchase.keys()) \
                .map(lambda purchase: Row(
                        purchase['userID'], 
                        purchase['gemPackID'], 
                        purchase['price'], 
                        Instant.of_epoch_milli(int(purchase['time'] * 1000)), 
                        Instant.of_epoch_milli(int(time.time() * 1000))), 
                    output_type=Types.ROW_NAMED(
                    ['user_id', 'gem_pack_id', 'price', 'purchase_time', 'process_time'],
                    [Types.INT(), Types.INT(), Types.FLOAT(), Types.INSTANT(), Types.INSTANT()]
                ))

        
        ad_stream = data_stream \
            .filter(lambda ad: "price" not in ad.keys()) \
            .map(lambda ad: Row(
                        ad['userID'], 
                        ad['gemPackID'],
                        Instant.of_epoch_milli(int(ad['time'] * 1000)), 
                        Instant.of_epoch_milli(int(time.time() * 1000))), 
                    output_type=Types.ROW_NAMED(
                    ['user_id', 'gem_pack_id', 'ad_time', 'process_time'],
                    [Types.INT(), Types.INT(), Types.INSTANT(), Types.INSTANT()]
                ))

        purchase_schema = Schema.new_builder() \
            .column('user_id', DataTypes.INT()) \
            .column('gem_pack_id', DataTypes.INT()) \
            .column('price', DataTypes.FLOAT()) \
            .column('purchase_time', DataTypes.TIMESTAMP_LTZ(3)) \
            .column('process_time', DataTypes.TIMESTAMP_LTZ(3)) \
            .watermark('process_time', 'SOURCE_WATERMARK()') \
            .build()


        purchase_table = table_env.from_data_stream(purchase_stream, purchase_schema)
        purchase_table.print_schema()

        ad_schema = Schema.new_builder() \
            .column('user_id', DataTypes.INT()) \
            .column('gem_pack_id', DataTypes.INT()) \
            .column('ad_time', DataTypes.TIMESTAMP_LTZ(3)) \
            .column('process_time', DataTypes.TIMESTAMP_LTZ(3)) \
            .watermark('process_time', 'SOURCE_WATERMARK()') \
            .build()

        ad_table = table_env.from_data_stream(ad_stream, ad_schema)
        ad_table.print_schema()

        table_env.register_table('ads', ad_table)
        table_env.register_table('purchases', purchase_table)


        table_env.execute_sql("""
            SELECT 
                user_id,
                gem_pack_id,
                SUM(price),
                MAX(purchase_time)
            FROM TABLE(TUMBLE(TABLE purchases, DESCRIPTOR(process_time), INTERVAL '4' SECONDS)) 
            GROUP BY 
                window_start,
                window_end,
                user_id,
                gem_pack_id
        """).print()

        # table_env.execute_sql("""
        #     SELECT 
        #         purchases.user_id,
        #         purchases.gem_pack_id,
        #         purchases.purchase_time,
        #         purchases.process_time,
        #         ads.ad_time,
        #         ads.process_time
        #     FROM (
        #         SELECT 
        #             * 
        #         FROM TABLE(TUMBLE(TABLE purchases, DESCRIPTOR(process_time), INTERVAL '4' SECONDS)) 
                
        #     ) purchases
        #     LEFT JOIN (
        #         SELECT * FROM TABLE(TUMBLE(TABLE ads, DESCRIPTOR(process_time), INTERVAL '4' SECONDS))
        #     ) ads
        #     ON purchases.user_id = ads.user_id AND purchases.gem_pack_id = ads.gem_pack_id
        # """).print()

        # table_env.execute_sql("SELECT * FROM ads").print()

        # table_env.execute_sql("""
        #     SELECT 
        #         user_id, 
        #         gem_pack_id, 
        #         TUMBLE_END(process_time, INTERVAL '4' SECOND) AS window_end
        #     FROM purchases
        #     GROUP BY 
        #         TUMBLE(process_time, INTERVAL '4' SECOND),
        #         user_id,
        #         gem_pack_id
        # """).print()

        # print('\nProcess Sink Schema')
        # aggregated.print_schema()

        # table_env.execute_sql("""
        #     CREATE TABLE aggregated (
        #         user_id INT,
        #         gem_pack_id INT,
        #         window_end TIMESTAMP(3)
        #     ) WITH (
        #         'connector' = 'print'
        #     )
        # """)

        # aggregated.execute_insert('aggregated').wait()

        # table_env.execute("Windowed Aggregation")

        #TABLE(TUMBLE(DATA => TABLE purchases, TIMECOL => DESCRIPTOR(process_time), SIZE => INTERVAL '4' SECOND))

        # table_env.execute_sql("""
        #     SELECT 
        #         p.user_id,
        #         p.gem_pack_id,
        #         purchase_time,
        #         p.process_time,
        #         ad_time,
        #         a.process_time
        #     FROM (
        #         SELECT * FROM TABLE(TUMBLE(TABLE purchases, DESCRIPTOR(process_time), INTERVAL '4' SECOND))
        #     ) p
        #     LEFT JOIN (
        #         SELECT * FROM TABLE(TUMBLE(TABLE ads, DESCRIPTOR(process_time), INTERVAL '4' SECOND))
        #     ) a
        #     ON p.user_id = a.user_id AND p.gem_pack_id = a.gem_pack_id
        # """).print()
            
            # table.add_columns()
            # table.map(lambda purchase: Row(purchase['gemPackID'], purchase['price'], purchase['time'], time.time()))  \
            #     .print_schema()     

        

    elif(args.mode == "aggregation"):
        aggregated_stream = data_stream \
            .filter(lambda purchase: "price" in purchase.keys()) \
            .map(lambda purchase: (purchase['gemPackID'], purchase['price'], purchase['time'], time.time())) \
            .assign_timestamps_and_watermarks(
                watermark_strategy = WatermarkStrategy 
                    .for_monotonous_timestamps()
                    .with_timestamp_assigner(ProcessingTimeAssigner())
            ) \
            .key_by(lambda i: i[0], key_type=Types.INT()) \
            .window(TumblingWindowAssigner(4, 4, False)) \
            .process(Aggregation(), result_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()])) \
            .map(Wrapper(), Types.STRING())
    
        file_sink = FileSink \
            .for_row_format('out', Encoder.simple_string_encoder()) \
            .with_output_file_config(
                OutputFileConfig.builder()
                    .with_part_prefix("data")
                    .with_part_suffix(".json")
                    .build()
            ) \
            .build()

        aggregated_stream.sink_to(file_sink)

    else:
        data_stream.print()

    environment.execute('socket_stream')
