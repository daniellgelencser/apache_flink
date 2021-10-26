import time
from multiprocessing import Queue
from typing import Any, Union

import numpy as np
import pandas as pd


class CreditcardDataGenerator:
    users: list[int]
    prices: list[float]
    packs: dict[int, float]
    pack_ids: list[int]

    def __init__(self, p_purchase, p_ad):
        self.data = pd.read_csv('./high_traffic.csv')
        self.t = 0
        self.max_t = self.data['time'].max()
        self.start_time = None

        self.object_types = ["purchase", "ad"]
        self.object_probabilities = {
            "purchase": p_purchase,
            "ad": p_ad,
        }

    def generate(self, queue: Queue, rate: int):
        """
        This function will fill the provided queue with purchase and ad instances.

        :param queue: The queue that will be filled with purchase instances
        :param rate: The rate is not used because it is determined by the data
        :return:
        """
        self.start_time = time.time()

        while self.t <= self.max_t:
            # retrieve an item from the data, cast it to a purcase of ad with according
            # probability. Than add it to the Queue.
            items = self.data[self.data['time'] == self.t].values
            for item in items:
                object_type = np.random.choice(
                    self.object_types,
                    p=list(self.object_probabilities.values())
                )

                queue.put(self.cast(item, object_type))

            next_event = (self.start_time + self.t + 1) - time.time()
            time.sleep(max(0.0, next_event))
            self.t += 1

    def purchase(self, user_id, pack_id, price) -> dict[str, Union[int, float]]:
        """
        This function wil create a single instance for a purchase
        The values represent (userID, gemPackID, price, time)

        :rtype: dict[str, Union[int, float]]
        """
        purchase = {
            'userID': int(user_id),
            'gemPackID': int(pack_id),
            'price': float(price),
            'time': float(time.time())
        }
        return purchase

    def ad(self, user_id, pack_id) -> dict[str, Union[int, float]]:
        """
        This function wil create a single instance for a ad
        The values represent (userID, gemPackID, time)

        :rtype: dict[str, Union[int, float]]
        """
        ad = {
            "userID": int(user_id),
            "gemPackID": int(pack_id),
            "time": float(time.time())
        }
        return ad

    def cast(self, item, object_type):
        if object_type == 'purchase':
            return self.purchase(item[0], item[1], item[2])
        else:
            return self.ad(item[0], item[1])
