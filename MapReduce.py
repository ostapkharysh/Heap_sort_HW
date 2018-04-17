
from multiprocessing import Pool, Manager, Lock
import multiprocessing
import os
import time

def file_reader(fl):
    with open(fl) as data:
        lines = list()
        n = 1
        for line in data:
            lines.append(line.strip().split(' '))
            n+=1
        return lines

def remove_file(fl):
    try:
        os.remove(fl)
    except FileNotFoundError:
        pass


def mapper(lns):
    #print(multiprocessing.current_process().name)
    #map_file_name = "MappersData/" + multiprocessing.current_process().name + ".txt"
    #print(lns)
    #f = open("Key/MapperFilesKeys.txt", "a")
    #map_key_list = file_reader("Key/MapperFilesKeys.txt")
    #if (map_file_name not in map_key_list):
    #    print(map_file_name)
    #    print(map_key_list)
    #    f.write(map_file_name + '\n')
    #    f.close()
    tup_line = list()
    for word in lns:
           tup_line.append((1, word.lower()))
           with open("MappersData/"+multiprocessing.current_process().name + ".txt", 'a') as FW:
               FW.write(word+",1"+'\n')
    return tup_line

def write_in_file(name, dic):
    with open(name, 'a') as F:
        for key, value in dic.items():
            F.write(key + ", "+str(value) + '\n')

def combiner(map_file):
    dict_words = dict()
    print(multiprocessing.current_process().name)
    with open("MappersData/"+map_file, 'r') as fl:
            #print("MappersData/"+map_file)
            for ln in fl:
                line_lst = ln.strip().split(",")
                try:
                    dict_words[line_lst[0]] += 1
                except KeyError:
                    dict_words[line_lst[0]] = 1
    write_in_file("CombinersData/" + multiprocessing.current_process().name + ".txt", dict_words)
    return dict_words

def init(l):
    global lock
    lock = l


remove_file("Key/MapperFilesKeys.txt")
file = 'data.txt'



out_lines = file_reader(file)
print(out_lines)

#print(out_lines)

#dict_line=list()

pool = Pool(processes=4,)
#m = multiprocessing.Manager()
#l = m.Lock()
#map_lock = partial(mapper, l)
#mapped_words = pool.map(mapper,out_lines)
#pool.close()
#pool.join()





#map_key_list = file_reader("Key/MapperFilesKeys.txt")
#print(map_key_list)
#file_lst = os.listdir("MappersData")
#func = partial(combiner, l)
#combine_words = pool.map(combiner, file_lst)

#sort = partition(itertools.chain(*mapped_words))
#print(time.time() - start_time)\
print(mapped_words)
print(combine_words)

#print(multiprocessing.current_process().name + ".txt")