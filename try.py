lst = [["d", 4], ["d", 3], ["d", 6], ["d", 2]]

summa = str(sum([el[1] for el in lst]))
print(summa)





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
