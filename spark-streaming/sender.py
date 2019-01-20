#Stampa podatke.

import time

with open("natl2014", "rb") as f:
    reader = f.readlines()
    for line in reader:
        print line
        time.sleep(0.001)
