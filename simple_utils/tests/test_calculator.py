import unittest

from simple_utils.calculator import (
    add,
    subtract,
    multiply,
    divide,
    power,
    sqrt,
    percentage,
    average,
    factorial,
)


class TestCalculator(unittest.TestCase):
    def test_basic_ops(self):
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(subtract(10, 4), 6)
        self.assertEqual(multiply(3, 5), 15)
        self.assertAlmostEqual(divide(10, 4), 2.5)
        self.assertEqual(power(2, 3), 8)

    def test_divide_by_zero(self):
        with self.assertRaises(ValueError):
            divide(1, 0)

    def test_sqrt(self):
        self.assertEqual(sqrt(9), 3)
        with self.assertRaises(ValueError):
            sqrt(-1)

    def test_percentage_and_average(self):
        self.assertEqual(percentage(200, 15), 30)
        self.assertAlmostEqual(average([1, 2, 3, 4]), 2.5)
        with self.assertRaises(ValueError):
            average([])

    def test_factorial(self):
        self.assertEqual(factorial(5), 120)
        with self.assertRaises(ValueError):
            factorial(-3)
        with self.assertRaises(ValueError):
            factorial(3.5)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()