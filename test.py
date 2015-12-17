__author__ = 'Asaf'
import os
# for item in os.listdir('solution'):
#     print item

for item in os.listdir('inputDataTask3/'):
    print item, os.path.isdir('inputDataTask3/' + item)

