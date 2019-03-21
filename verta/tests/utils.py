import random

import requests


def gen_str(length=8):
    return ''.join([chr(random.randrange(97, 123))
                    for _
                    in range(length)])


def gen_int(start=10, stop=None):
    return random.randrange(start, stop)


def gen_float(start=1, stop=None):
    if stop is None:
        return random.random()*start
    else:
        return random.uniform(start, stop)
