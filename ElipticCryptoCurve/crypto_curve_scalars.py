# curve in Weierstrass form: y^2 = x^3 + ax + b

import math
import gmpy


def check_if_on_curve(x, y, a, b, mod):
    left = y ** 2
    right = x ** 3 + a * x + b
    return left % mod == right % mod


class Point:

    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.a = 3
        self.b = 4
        self.mod = 17  # PRIME NUMBER ONLY

    def add(self, other):

        """
        Addition of two Points
        """

        P1_on_curve = check_if_on_curve(self.x, self.y, self.a, self.b, self.mod)
        P2_on_curve = check_if_on_curve(other.x, other.y, other.a, other.b, other.mod)

        if not P1_on_curve or not P2_on_curve:
            print("Unable to conduct the addition. A least one of these points is not on the curve.")
            return (Point(0, 0))

        if isinstance(self.x, str) and isinstance(other, str):
            return Point(math.inf(), math.inf())

        elif isinstance(self, str):
            return other

        elif isinstance(other, str):
            return self

        elif (self.x == other.x) and (self.y == other.y):
            num = 3 * self.x ** 2 + self.a
            denom = 2 * self.y

        elif self.x == other.x:
            return Point(math.inf, math.inf)
        else:
            num = other.y - self.y  # (y2 - y1)
            denom = other.x - self.x  # (x2 - x1)

        denom_mi = gmpy.invert(denom, self.mod)
        lmb = (num * denom_mi) % self.mod

        x3 = (lmb ** 2 - other.x - self.x) % self.mod
        y3 = (lmb * (self.x - x3) - self.y) % self.mod

        return Point(x3, y3)


#On the Curve
P_1 = Point(5, 5)
P_2 = Point(6, 0)

P_3 = P_1.add(P_2)

print(P_3.x, P_3.y)

print()

P_1 = Point(2, 4) # Not on the Curve
P_2 = Point(23, 1)

P_3 = P_1.add(P_2)

print(P_3.x, P_3.y)
