import time
from multiprocessing import Queue
from typing import Any, Union

import numpy as np


class Generator:
    users: list[int]
    prices: list[float]
    packs: dict[int, float]
    pack_ids: list[int]

    def __init__(self, p_purchase, p_ad):
        self.users = [i for i in range(10)]
        self.prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]
        self.generator_functions = {
            "purchase": self.gen_purchase,
            "ad": self.gen_ad
        }
        self.generator_probabilities = {
            "purchase": p_purchase,
            "ad": p_ad,
        }

    def generate(self, queue: Queue, rate: int):
        """
        This function will fill the provided queue with purchase instances.

        :param queue: The queue that will be filled with purchase instances
        :param rate: The rate for the throughput of the system in generations per second
        :return:
        """
        print("Start generating data")

        while True:
            start_time = time.time()

            # Generate a random instance of a purchase with according probability
            item = self.generator_functions[
                np.random.choice(
                    list(self.generator_functions.keys()),
                    p=list(self.generator_probabilities.values())
                )
            ]()
            print('Object created: ' + str(item))

            queue.put(item)
            end_time = time.time()
            time.sleep(max(0.0, 1 / rate - (end_time - start_time)))

    def gen_purchase(self) -> dict[str, Union[int, float]]:
        """
        This function wil generate a single instance for a purchase
        The values represent (userID, gemPackID, price, time)

        :rtype: dict[str, Union[int, float]]
        """
        user_id: int = np.random.choice(self.users)
        pack_id: int = np.random.choice(self.pack_ids)

        purchase = {
            'userID': int(user_id),
            'gemPackID': int(pack_id),
            'price': float(self.packs[pack_id]),
            'time': float(time.time())
        }
        return purchase

    def gen_ad(self) -> dict[str, Union[int, float]]:
        """
        This function wil generate a single instance for a ad
        The values represent (userID, gemPackID, time)

        :rtype: dict[str, Union[int, float]]
        """
        user_id: int = np.random.choice(self.users)
        pack_id: int = np.random.choice(self.pack_ids)

        ad = {
            "userID": int(user_id),
            "gemPackID": int(pack_id),
            "time": float(time.time())
        }
        return ad
