"""10.04.2018"""

#author Ostap Kharysh

"""
Map Reduce implementation containing: Map, Combine, Shuffle&Sort, Reduce phases.
The Architecture is designed to provide tracking of how the data changes during phases
mentioned above. Idea of such implementation is also based on thoughts that MapReduce algorithm
could be run on distributed computers so each of them should store their operation results in DB
 (.txt files in my case). There are also directories each of them contains data of specific phase.
"""

"""
Try to run: python3 MapReduce.py -i data.txt -o foo.txt -m 4  --c y
"""



import getopt
from multiprocessing import Pool
import multiprocessing, sys, os, csv


def file_reader(fl):
    """
    :param fl: input file with words
    :return: returns the array of words
    """
    data = list()
    if fl.endswith('.csv'):
        with open(fl, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in spamreader:
                data += (', '.join(row))
    else:
        with open(fl, 'r') as file:
            for line in file:
                data +=line.strip().lower().split(' ')
    return data

def remove_file(fl):
    """
    :param fl: file
    :return:  removes file from the system
    """
    try:
        os.remove(fl)
    except FileNotFoundError:
        pass


def mapper(lns):
    """
    :param lns: takes words
    :return: files of data chunks
    """
    tup_line = list()
    for word in lns:
        tup_line.append((1, word.lower()))
        with open("MappersData/" + multiprocessing.current_process().name + ".txt", 'a') as FW:
            FW.write(word + ",1" + '\n')
    return tup_line


def write_in_file(name, dic):
    """
    :param file name:
    :param dictionary of words and their count:
    :return file or words and their frequencies:
    """
    with open(name, 'a') as F:
        for key, value in dic.items():
            F.write(key + ","+str(value) + '\n')

def combiner(map_file):
    """
    :param takes mapper chanks of data:
    :return files of combined words from chunks:
    """
    dict_words = dict()
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
    """
    :param takes the names of intended files to be shuffled:
    :return returns files of shuffled & sorted words:
    """

    data = []
    comb_files = os.listdir(directory)
    for chunk in comb_files:
            with open(directory+'/' + chunk) as fl:
                for line in fl:
                    data.append(line.strip().split(","))

    data.sort(key=lambda x:x[0])

    for i in range(len(data)):
            with open('ShufflersData/Shuffle_' + data[i][0] + ".txt", 'a') as FW:
                FW.write(data[i][0]+","+data[i][1]+'\n')

def reducer(shuf_file):
    """
    :param takes files of shuffled words
    :return: return the words and their frequencies in files
    """
    data =[]
    with open('ShufflersData/'+ shuf_file) as fl:
        for line in fl:
            data.append(line.strip().split(','))
    with open("ReducersData/Reduce_"+shuf_file, 'a') as F:
        if len(data) == 1:
            F.write(data[0][0]+','+data[0][1])
            pass
        else:
            pass
            F.write(data[0][0]+','+str(sum([int(el[1]) for el in data])))

def outer(files, outputfile):
    """
    :param files: takes all reduced files
    :param outputfile: intended name of file od all words and their numbers
    :return:  file with words and their frequencies
    """
    with open(outputfile, 'a') as fl:
        for file in files:
            with open('ReducersData/'+file) as source:
                fl.write(source.readline() +'\n')
    print("Words counted in this document are here: "+outputfile)


def divide(seq, num):
    """
    :param seq: data list of words
    :param num: prefered divider
    :return: divided sequence of data
    """
    avg = round(len(seq) /num)
    out = []
    last = 0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def map_reduce( infile, outfile, mappers_number, reducers_number, combine='y'):
    """

    :param infile: file taken for words' count
    :param outfile: file take to store results of wordcount
    :param mappers_number: number or mappers
    :param reducers_number: number of reducers
    :param combine: checker to do combine phase or not
    :return: returns file of counted words
    """
    COMBINERS_STORAGE = "CombinersData"
    MAPPERS_STORAGE = "MappersData"
    SHUFFLERS_STORAGE = "ShufflersData"
    REDUCERS_STORAGE = "ReducersData"

    os.mkdir(COMBINERS_STORAGE)
    os.mkdir(MAPPERS_STORAGE)
    os.mkdir(SHUFFLERS_STORAGE)
    os.mkdir(REDUCERS_STORAGE)

    out_lines = file_reader(infile)

    if type(mappers_number) == str:
       mappers_number=round(len(out_lines)/5 +1)

    division = divide(out_lines, mappers_number)

    pool = Pool(processes=mappers_number, )
    pool.map(mapper, division)

    file_lst = os.listdir(MAPPERS_STORAGE)
    if combine !='n':
        pool.map(combiner, file_lst)
        shuffler(COMBINERS_STORAGE)
    else:
        shuffler(MAPPERS_STORAGE)

    reduce_lst = os.listdir(SHUFFLERS_STORAGE)
    if type(reducers_number) == str:
        reducers_number = round(len(reduce_lst)/5 +1)

    pool2 = Pool(processes=reducers_number, )

    pool2.map(reducer, reduce_lst)

    outer(os.listdir(REDUCERS_STORAGE), outfile)



def run(argv):
    mappers_number = '(Not specified) Will depend on the input file size'
    reducer_number = '(Not specified) Will depend on the input file size'
    combine = '(Not specified) yes'
    inputfile = 'file.<txt, csv>'
    outputfile = 'CountedWords.txt'

    try:
        opts, args = getopt.getopt(argv, "i:o:m:r:c", [ "inputfile=","outputfile=" "mappers=", "reducers=","combine="])
    except getopt.GetoptError:
        print('-i <inputfile<csv,txt>> -o <outputfile<.txt>> -m <mappers number> -r <reducers number> --c <combine?<y,n>>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-i <inputfile<csv,txt>> -o <outputfile<.txt>> -m <mappers number> -r <reducers number> --c <combine?<y,n>>')
            sys.exit()
        elif opt == '-i':
            inputfile = arg
        elif opt == '-o':
            outputfile = arg
        elif opt in ("-m", "--mappers"):
            mappers_number = int(arg)
        elif opt in ("-r", "--reducers"):
            reducer_number = int(arg)
        elif opt in ('--c', "--combine"):
            combine = arg
        if inputfile == 'file.<txt, csv>':
            print("Input file should be specified: -i file.<txt, csv>")
            sys.exit()
    print('Input file: ', inputfile)
    print('Output file: ', outputfile)
    print('Number of mappers: ', mappers_number)
    print('Number of reducers: ', reducer_number)
    print('Combine: ', combine)
    map_reduce(inputfile, outputfile, mappers_number, reducer_number, combine)


if __name__=="__main__":
    run(sys.argv[1:])
