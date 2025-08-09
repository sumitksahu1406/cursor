from __future__ import annotations

from math import sqrt as _sqrt, factorial as _factorial
from typing import Iterable, Sequence


def add(a: float, b: float) -> float:
    return a + b


def subtract(a: float, b: float) -> float:
    return a - b


def multiply(a: float, b: float) -> float:
    return a * b


def divide(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("Division by zero is not allowed")
    return a / b


def power(a: float, b: float) -> float:
    return a ** b


def sqrt(x: float) -> float:
    if x < 0:
        raise ValueError("Square root of negative number is not allowed")
    return _sqrt(x)


def percentage(base: float, percent: float) -> float:
    return base * (percent / 100.0)


def average(numbers: Sequence[float] | Iterable[float]) -> float:
    values = list(numbers)
    if len(values) == 0:
        raise ValueError("Cannot compute average of empty sequence")
    return sum(values) / len(values)


def factorial(n: int) -> int:
    if not isinstance(n, int):
        raise ValueError("factorial is only defined for integers")
    if n < 0:
        raise ValueError("factorial is not defined for negative values")
    return _factorial(n)