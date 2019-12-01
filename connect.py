from time import sleep
from random import random


def wait(delay=2, variation=1):
    m, x, c = variation, random(), delay - variation / 2
    sleep(m * x + c)

SITE = 'http://www.whoscored.com'
HEADERS = {'User-Agent': 'Mozilla/5.0'}