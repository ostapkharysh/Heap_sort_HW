import math
class Median:
    def __init__(self):
        self.low_array = []
        self.high_array = []

    ###################################################################################################################
    """MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP MAX HEAP"""
    ###################################################################################################################

    def max_heap_insert(self, key):
        self.low_array += [None]
        self.low_array[len(self.low_array)-1] = -math.inf
        self.heap_increase_key(len(self.low_array)-1, key)

    def heap_increase_key(self, i, key):
        if key < self.low_array[i]:
            return SyntaxError("new key smaller than current key")
        self.low_array[i] = key
        while i > 0 and self.low_array[self.Parent(i)] < self.low_array[i]:
            self.low_array[i], self.low_array[self.Parent(i)] = self.low_array[self.Parent(i)], self.low_array[i]
            i = self.Parent(i)

    def build_max_heap(self):
        for i in range(len(self.low_array) // 2, -1, -1):
            self.max_heapify(i)

    def max_heapify(self, i):
        l = self.Left(i)
        r = self.Right(i)
        if l < len(self.low_array) and self.low_array[l] > self.low_array[i]:
            largest = l
        else:
            largest = i
        if r < len(self.low_array) and self.low_array[r] > self.low_array[largest]:
            largest = r
        if largest != i:
            self.low_array[i], self.low_array[largest] = self.low_array[largest], self.low_array[i]
            self.max_heapify(largest)


    def heap_extract_max(self):
        maximum = self.low_array[0]
        self.low_array.pop(0)
        self.max_heapify(0)
        return maximum

    ###################################################################################################################
    """MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP MIN HEAP"""
    ###################################################################################################################


    def min_heapify(self, i):
        l = self.Left(i)
        r = self.Right(i)
        if l < len(self.high_array) and self.high_array[l] < self.high_array[i]:
            smallest = l
        else:
            smallest = i
        if r < len(self.high_array) and self.high_array[r] < self.high_array[smallest]:
            smallest = r
        if smallest != i:
            self.high_array[i], self.high_array[smallest] = self.high_array[smallest], self.high_array[i]
            self.min_heapify(smallest)

    def build_min_heap(self):
        for i in range(len(self.low_array) // 2, -1, -1):
            self.min_heapify(i)

    def min_heap_insert(self, key):
        self.high_array += [None]
        self.high_array[len(self.high_array)-1] = +math.inf
        self.heap_decrease_key(len(self.high_array)-1, key)

    def heap_decrease_key(self, i, key):
        if key > self.high_array[i]:
            return SyntaxError("new key bigger than current key")
        self.high_array[i] = key
        while i > 0 and self.high_array[self.Parent(i)] > self.high_array[i]:
            self.high_array[i], self.high_array[self.Parent(i)] = self.high_array[self.Parent(i)], self.high_array[i]
            i = self.Parent(i)

    def heap_extract_min(self):
        minimum = self.high_array[0]
        self.high_array.pop(0)
        self.min_heapify(0)
        return minimum
    ###################################################################################################################
    """EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING EVERYTHING"""
    ###################################################################################################################


    def add_element(self, value):
        if self.low_array == []:
            self.max_heap_insert(value)
            self.build_max_heap()
        elif value < self.low_array[0]:
            self.max_heap_insert(value)
            self.build_max_heap()
        else:
            self.min_heap_insert(value)
            self.build_min_heap()

        if len(self.low_array) - len(self.high_array) > 1:
            self.min_heap_insert(self.heap_extract_max())
            self.build_max_heap()
            self.build_min_heap()
        elif len(self.high_array) - len(self.low_array)>1:
            self.max_heap_insert(self.heap_extract_min())
            self.build_max_heap()
            self.build_min_heap()

    def get_median(self):
        if len(self.low_array) - len(self.high_array) == 0:
            return (self.low_array[0], self.high_array[0])
        else:
            if len(self.low_array) > len(self.high_array):
                return (self.low_array[0])
            else:
                return (self.high_array[0])

    def get_maxheap_elements(self):
        return self.low_array

    def get_minheap_elements(self):
        return self.high_array


    def Left(self,i):
        return i * 2 + 1


    def Right(self, i):
        return i * 2 + 2


    def Parent(self, i):
        return i // 2


piramid = Median()


piramid.add_element(22)
piramid.add_element(34)
piramid.add_element(3)
piramid.add_element(45)
piramid.add_element(2)
piramid.add_element(4)
piramid.add_element(12)
piramid.add_element(27)
piramid.add_element(134)
piramid.add_element(8)
piramid.add_element(56)
piramid.add_element(23)
piramid.add_element(76)
piramid.add_element(132)
piramid.add_element(98)
piramid.add_element(1)
piramid.add_element(5)


print(piramid.high_array)
print(piramid.low_array)
print(piramid.get_median())
