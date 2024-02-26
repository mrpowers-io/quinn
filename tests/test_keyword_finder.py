from quinn.keyword_finder import search_file, search_files, keyword_format, surround_substring


def test_search_file():
    file_path = "tests/test_files/some_pyspark.py"
    
    results = search_file(file_path)
    assert results[file_path] == 8


def test_search_files():
    results = search_files("tests/test_files")

    assert results["tests/test_files/some_pyspark.py"] == 8
    assert results["tests/test_files/good_schema1.csv"] == 0


def test_keyword_format():
    print(keyword_format("spark rdd stuff"))
    print(keyword_format("spark rdd stuff with bad _jvm"))
    print(keyword_format("nice string"))
    print(keyword_format(""))


def test_surround_substring():

    assert "spark **rdd|| stuff" == surround_substring("spark rdd stuff", "rdd", "**", "||")
    assert "spark **rdd|| stuff with **rdd||" == surround_substring("spark rdd stuff with rdd", "rdd", "**", "||")
    assert "spark **rdd||dd stuff" == surround_substring("spark rdddd stuff", "rdd", "**", "||")


