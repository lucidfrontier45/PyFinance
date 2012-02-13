#!/usr/bin/python

import sys
import scipy as sp
import random

length = int(sys.argv[1])
phi = -0.8
x = 100
sigma = 0.001
dx1 = 0
dx2 = 0

for i in range(length):
  dx1 = dx2 * phi + random.gauss(0,sigma)
  x = x + dx1
  dx2 = dx1
  print x
