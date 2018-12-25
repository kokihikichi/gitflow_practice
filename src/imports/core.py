import os, random, itertools, time
import numpy as np
from codecs import open
import pickle
from tqdm import tqdm, tnrange, tqdm_notebook
from sklearn.metrics import roc_auc_score