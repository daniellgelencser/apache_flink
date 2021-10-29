import argparse
import math

from driver.NetworkStreamer import NetworkStreamer
from driver.Generator import Generator
from driver.DataQueue import DataQueue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9994)
    parser.add_argument('--budget', type=int, default=100_000_000)
    parser.add_argument('--rate', type=int, default=3000, help='rate in transactions per second')
    parser.add_argument('--generators', type=int, default=16, help='number of generators')
    parser.add_argument('--max_queue_size', type=int, default=10_000,
                        help='maximum queue size for the generated transactions')
    parser.add_argument('--p-purchase', type=float, default=0.5,
                        help="probability of an purchase to be generated, this value and --p-ad have to sum up to 1 ")
    parser.add_argument('--p-ad', type=float, default=0.5,
                        help="probability of an ad to be generated, this value and --p-purchase have to sum up to 1 ")

    args = parser.parse_args()

    if args.p_purchase + args.p_ad != 1.0:
        print('--p-purchase and --p-ad do not sum up to 1 but to {}'.format(args.p_purchase + args.p_ad))
        exit(1)

    generator = Generator(args.p_purchase, args.p_ad)
    data_queue = DataQueue(generator,
                           args.generators,
                           args.rate,
                           math.ceil(args.budget / args.max_queue_size),
                           args.budget)
    streamer = NetworkStreamer(args.host, args.port, data_queue)
    streamer.run()
