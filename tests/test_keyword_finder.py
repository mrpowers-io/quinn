import pytest
from quinn.keyword_finder import search_file, search_files

def test_search_file():
    search_file("tests/test_files/some_pyspark.py")

def test_search_files():
    search_files("tests/test_files")