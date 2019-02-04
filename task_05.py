
preorder = [5, 6, 7, 3, 0, 0, 4, 0, 0, 0, 2, 1, 8, 0, 0, 0, 9, 0, 10, 0, 0]

class Node:
    def __init__(self, data):
        self.left = None
        self.right = None
        self.data = data


class BinarySearchTree:
    def __init__(self, preorder):
        self.preorlst = preorder
        self.root = None
        self.node = Node
        self.left = None
        self.right = None

    def create_binary_tree(self):
        def final_left(node):
            global i
            i = 1
            while node.data != 0:
                node.left = preorder[i]
                node.left.parent = node
                i += 1
                return final_left(node.left)
            node.left = None
            return node

        node = self.node
        node.data = preorder[0]
        result = final_left(node)
        i
