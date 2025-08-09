import unittest

from simple_utils.text_processor import (
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


class TestTextProcessor(unittest.TestCase):
    def test_counts(self):
        self.assertEqual(word_count("Hello, world!"), 2)
        self.assertEqual(char_count("ab c\n\t"), 5)
        self.assertEqual(char_count("ab c\n\t", include_spaces=False), 3)
        self.assertEqual(sentence_count("One. Two? Three!"), 3)

    def test_replace_and_case(self):
        self.assertEqual(find_replace("foo bar baz", "bar", "qux"), "foo qux baz")
        self.assertEqual(to_upper("abc"), "ABC")
        self.assertEqual(to_lower("ABC"), "abc")

    def test_capitalize_and_unique(self):
        text = "hello. how are you? i'm fine!"
        self.assertEqual(
            capitalize_sentences(text),
            "Hello. How are you? I'm fine!",
        )
        uniq = unique_words("Apple banana apple", lower=True)
        self.assertEqual(uniq, ["apple", "banana"])

    def test_reverse_and_palindrome(self):
        self.assertEqual(reverse_words("one two three"), "three two one")
        self.assertTrue(is_palindrome("A man, a plan, a canal: Panama"))
        self.assertFalse(is_palindrome("Hello"))


if __name__ == "__main__":
    unittest.main()