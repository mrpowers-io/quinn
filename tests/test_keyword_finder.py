from quinn.keyword_finder import search_file, search_files, keyword_format, surround_substring


def test_search_file():
    search_file("tests/test_files/some_pyspark.py")


def test_search_files():
    search_files("tests/test_files")


def test_keyword_format():
    print(keyword_format("spark rdd stuff"))
    print(keyword_format("spark rdd stuff with bad _jvm"))
    print(keyword_format("nice string"))
    print(keyword_format(""))


def test_surround_substring():
    print(surround_substring("spark rdd stuff", "rdd", "**", "||"))
