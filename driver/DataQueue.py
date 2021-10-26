import math
from multiprocessing import Process, Queue
from typing import Generator


class DataQueue:
    """
    This class initializes the data generator and provides data for the Software Under Test (SUT)

    :param consumer: the consumer that handles the data processing
    :param n_generators: number of generators that will fill the queue
    :param max_queue_size: maximum size of the queue
    :param budget: this budged will be distributed over the generators
    """
    budget: int
    n_generators: int
    queue: Queue
    generators: list[Generator]

    def __init__(self, generator,  n_generators: int, rate: int, max_queue_size: int, budget: int):
        self.n_generators = n_generators
        self.rate = rate
        self.budget = budget
        self.queue = Queue(max_queue_size)

        # generator for the benchmark
        # generator = BenchmarkGenerator(p_purchase, p_ad)

        # These generators wil fill up the queue with purchase instances
        # in there own separate threads. Every generator generates an equal
        # amount of instances. Note: the generator may go over budget because
        # the budget is divided into equal parts and rounded up. Same holds for the rate
        self.generators = [
            Process(
                target=generator.generate,
                args=(self.queue, math.ceil(self.rate / self.n_generators)),
                daemon=True
            )
            for _ in range(self.n_generators)
        ]

    def start(self) -> None:
        """
        Starts the generator and runs the benchmark
        """
        print("Start data queue")
        for generator in self.generators:
            generator.start()

    def next(self):
        return self.queue.get()

    def get_budget(self):
        return self.budget
