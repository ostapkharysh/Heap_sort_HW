
from multiprocessing import Pool, Manager, Lock
import multiprocessing
import os
import time

def file_reader(fl):
    data = list()
    with open(fl, 'r') as file:
        for line in file:
            data +=line.strip().split(' ')
    return data

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
    print("Mapper")
    print(multiprocessing.current_process().name)
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
    print("Combiner")
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

def shuffle_sort(comb_file):


def divide(seq, num):
    avg = round(len(seq) /num)
    out = []
    last = 0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


remove_file("Key/MapperFilesKeys.txt")
file = 'data.txt'

mappers_number = 3

out_lines = file_reader(file)
#print(out_lines)
division = divide(out_lines, mappers_number)
print(division)
#print(out_lines)


"""
#################################
output = multiprocessing.Queue()

#MAP
map_processes = [multiprocessing.Process(target=mapper, args=([division[x]])) for x in range(mappers_number)]

# Run processes
for p in map_processes:
    p.start()

# Exit the completed processes
for p in map_processes:
    p.join()

file_lst = os.listdir("MappersData")

#Combine
combine_processes = [multiprocessing.Process(target=combiner, args=([file_lst[x]])) for x in range(mappers_number)]


# Run processes
for p in combine_processes:
    p.start()

# Exit the completed processes
for p in combine_processes:
    p.join()

##################################################

"""









#dict_line=list()py

pool = Pool(processes=mappers_number,)
#m = multiprocessing.Manager()
#l = m.Lock()
#map_lock = partial(mapper, l)
mapped_words = pool.map(mapper,division)
#pool.close()
#pool.join()





#map_key_list = file_reader("Key/MapperFilesKeys.txt")
#print(map_key_list)
file_lst = os.listdir("MappersData")
#func = partial(combiner, l)
combine_words = pool.map(combiner, file_lst)

#sort = partition(itertools.chain(*mapped_words))
#print(time.time() - start_time)\
print(mapped_words)
print(combine_words)

#print(multiprocessing.current_process().name + ".txt")