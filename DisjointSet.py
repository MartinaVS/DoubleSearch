import random

class DisjointSet:
    def __init__(self, n):
        self.n = n
        self.vertices = list(range(n))

    def find(self, item):
        if self.vertices[item] == item:
            return item
        else:
            res = self.find(self.vertices[item])
            self.vertices[item] = res
            return res
            # return self.find(self.vertices[item])
            # return self.vertices[item] = self.find(self.vertices[item])

    def union(self, set1, set2):
        root1 = self.find(set1)
        root2 = self.find(set2)
        if random.randint(1, 2) == 1:
            root2, root1 = root1, root2
        self.vertices[root1] = root2

    def colors_list(self):
        return [self.find(i) for i in range(self.n)]
