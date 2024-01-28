""" 'Testing Package' add path to tested source to the module search path """
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
