from __future__ import annotations

import argparse
import json
from typing import Any

from . import (
    add,
    subtract,
    multiply,
    divide,
    power,
    sqrt,
    percentage,
    average,
    factorial,
    word_count,
    char_count,
    sentence_count,
    find_replace,
    to_upper,
    to_lower,
    capitalize_sentences,
    unique_words,
    reverse_words,
    is_palindrome,
)


def _print(value: Any) -> None:
    # Pretty-print complex values as JSON; primitives as plain text
    if isinstance(value, (list, dict, tuple)):
        print(json.dumps(value, ensure_ascii=False, indent=2))
    else:
        print(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simple Utils CLI")
    subparsers = parser.add_subparsers(dest="domain", required=True)

    # Calculator
    calc = subparsers.add_parser("calc", help="Calculator operations")
    calc_sub = calc.add_subparsers(dest="op", required=True)

    def num_pair(p):
        p.add_argument("a", type=float)
        p.add_argument("b", type=float)

    num_pair(calc_sub.add_parser("add", help="a + b"))
    num_pair(calc_sub.add_parser("sub", help="a - b"))
    num_pair(calc_sub.add_parser("mul", help="a * b"))

    div_p = calc_sub.add_parser("div", help="a / b")
    num_pair(div_p)

    pow_p = calc_sub.add_parser("pow", help="a ** b")
    num_pair(pow_p)

    sqrt_p = calc_sub.add_parser("sqrt", help="sqrt(a)")
    sqrt_p.add_argument("a", type=float)

    perc_p = calc_sub.add_parser("perc", help="percent of base")
    perc_p.add_argument("base", type=float)
    perc_p.add_argument("percent", type=float)

    avg_p = calc_sub.add_parser("avg", help="average of numbers")
    avg_p.add_argument("numbers", type=float, nargs="+", help="one or more numbers")

    fact_p = calc_sub.add_parser("fact", help="factorial of n (int >= 0)")
    fact_p.add_argument("n", type=int)

    # Text
    text = subparsers.add_parser("text", help="Text processing operations")
    text_sub = text.add_subparsers(dest="op", required=True)

    wc = text_sub.add_parser("wcount", help="word count")
    wc.add_argument("text")

    cc = text_sub.add_parser("ccount", help="char count")
    cc.add_argument("text")
    cc.add_argument("--no-spaces", action="store_true", help="exclude whitespace from count")

    sc = text_sub.add_parser("scount", help="sentence count")
    sc.add_argument("text")

    rp = text_sub.add_parser("replace", help="find & replace")
    rp.add_argument("text")
    rp.add_argument("find")
    rp.add_argument("replace")

    up = text_sub.add_parser("upper", help="uppercase")
    up.add_argument("text")

    low = text_sub.add_parser("lower", help="lowercase")
    low.add_argument("text")

    cap = text_sub.add_parser("cap", help="capitalize sentences")
    cap.add_argument("text")

    uniq = text_sub.add_parser("unique", help="unique words (sorted)")
    uniq.add_argument("text")
    uniq.add_argument("--keep-case", action="store_true")

    rw = text_sub.add_parser("rwords", help="reverse words")
    rw.add_argument("text")

    pal = text_sub.add_parser("pal", help="is palindrome")
    pal.add_argument("text")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.domain == "calc":
            if args.op == "add":
                _print(add(args.a, args.b))
            elif args.op == "sub":
                _print(subtract(args.a, args.b))
            elif args.op == "mul":
                _print(multiply(args.a, args.b))
            elif args.op == "div":
                _print(divide(args.a, args.b))
            elif args.op == "pow":
                _print(power(args.a, args.b))
            elif args.op == "sqrt":
                _print(sqrt(args.a))
            elif args.op == "perc":
                _print(percentage(args.base, args.percent))
            elif args.op == "avg":
                _print(average(args.numbers))
            elif args.op == "fact":
                _print(factorial(args.n))
            else:
                parser.error("Unknown calc operation")

        elif args.domain == "text":
            if args.op == "wcount":
                _print(word_count(args.text))
            elif args.op == "ccount":
                _print(char_count(args.text, include_spaces=not args.no_spaces))
            elif args.op == "scount":
                _print(sentence_count(args.text))
            elif args.op == "replace":
                _print(find_replace(args.text, args.find, args.replace))
            elif args.op == "upper":
                _print(to_upper(args.text))
            elif args.op == "lower":
                _print(to_lower(args.text))
            elif args.op == "cap":
                _print(capitalize_sentences(args.text))
            elif args.op == "unique":
                _print(unique_words(args.text, lower=not args.keep_case))
            elif args.op == "rwords":
                _print(reverse_words(args.text))
            elif args.op == "pal":
                _print(is_palindrome(args.text))
            else:
                parser.error("Unknown text operation")
        return 0
    except Exception as exc:  # noqa: BLE001
        parser.exit(status=2, message=f"Error: {exc}\n")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())