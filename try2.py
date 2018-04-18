
from multiprocessing import Pool, Manager, Lock
import multiprocessing
import os



def file_reader(fl):
    data = list()
    with open(fl, 'r') as file:
        for line in file:
            data +=line.strip().lower().split(' ')
    return data

def remove_file(fl):
    try:
        os.remove(fl)
    except FileNotFoundError:
        pass


def mapper(lns):
    """
    print("Mapper")
    print(multiprocessing.current_process().name)
    tup_line = list()
    map_filename = "Data/Mapper"+multiprocessing.current_process().name + ".txt"
    FW = open(map_filename, 'a')
    for word in lns:
        tup_line.append((1, word.lower()))
        FW.write(word+",1"+'\n')
    FW.close()
    with open("Key/Mappers.txt", "a") as fl:
        fl.write(map_filename)

    return tup_line
    """
    print("Mapper")
    print(multiprocessing.current_process().name)
    tup_line = list()
    for word in lns:
        tup_line.append((1, word.lower()))
        with open("MappersData/" + multiprocessing.current_process().name + ".txt", 'a') as FW:
            FW.write(word + ",1" + '\n')
    return tup_line


def write_in_file(name, dic):
    with open(name, 'a') as F:
        for key, value in dic.items():
            F.write(key + ","+str(value) + '\n')

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

def shuffler(directory):
    print("SHUFFLE")
    data = []
    comb_files = os.listdir(directory)
    for chunk in comb_files:
            with open(directory+'/' + chunk) as fl:
                for line in fl:
                    data.append(line.strip().split(","))

    data.sort(key=lambda x:x[0])

    for i in range(len(data)):
            with open('Data/Shuffle_' + data[i][0] + ".txt", 'a') as FW:
                FW.write(data[i][0]+","+data[i][1]+'\n')

def reducer(shuf_file):
    print("Reducer")
    print(shuf_file)
    data =[]
    with open('Data/'+ shuf_file) as fl:
        for line in fl:
            data.append(line.strip().split(','))
    with open("ReducersData/Reduce_"+shuf_file, 'a') as F:
        if len(data) == 1:
            F.write(data[0][0]+','+data[0][1])
            pass
        else:
            pass
            F.write(data[0][0]+','+str(sum([int(el[1]) for el in data])))

def outer(files):
    with open("WordsCount.txt", 'a') as fl:
        for file in files:
            with open('ReducersData/'+file) as source:
                fl.write(source.readline() +'\n')


def divide(seq, num):
    avg = round(len(seq) /num)
    out = []
    last = 0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out



file = 'data.txt'

os.mkdir("CombinersData")
os.mkdir("MappersData")
os.mkdir("Data")
os.mkdir("ReducersData")

mappers_number = 3

out_lines = file_reader(file)
#print(out_lines)
division = divide(out_lines, mappers_number)
print(division)
#print(out_lines)


pool = Pool(processes=mappers_number,)
mapped_words = pool.map(mapper,division)


file_lst = os.listdir("MappersData")
combine_words = pool.map(combiner, file_lst)


print(mapped_words)
print(combine_words)


shuffler("CombinersData")

reducer_number = 6
reduce_lst = os.listdir("Data")
pool2 = Pool(processes=reducer_number,)


print(reduce_lst)
pool2.map(reducer,reduce_lst)

outer(os.listdir("ReducersData"))