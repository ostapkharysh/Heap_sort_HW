# curve in Weierstrass form: y^2 = x^3 + ax + b

import math


class Point:

    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.a = 1

    def double(self):
        """
        Used in case two points are equal
        """
        try:
            lmb = (3 * self.x ** 2 + self.a) / (2 * self.y)
        except ZeroDivisionError:
            return Point(math.inf, math.inf)
        x3 = lmb**2 - 2 * self.x
        y3 = lmb * (self.x - x3) - self.y
        return Point(x3, y3)

    def add(self, other):
        """
        Addition of two Points
        """
        if self.x == other.x:
            if self.y == other.y:
                return self.double()
            else:
                return Point(math.inf, math.inf)
        else:
            try:
                lmb = (other.y - self.y) / (other.x - self.x)
            except ZeroDivisionError:
                return Point(math.inf, math.inf)
            x3 = lmb ** 2 - other.x - self.x
            y3 = -(other.y + lmb * (x3 - other.x))
            return Point(x3, y3)


P_1 = Point(1, 5)
P_2 = Point(5, 1)

P_3 = P_1.add(P_2)

print(P_3.x, P_3.y)
