from quinn.keyword_finder import search_file, search_files, keyword_format, surround_substring


def test_search_file():
    file_path = "tests/test_files/some_pyspark.py"
    results = search_file(file_path)

    assert results["word_count"]["rdd"] == 5
    assert results["word_count"]["sparkContext"] == 2


def test_search_files():
    results = search_files("tests/test_files")

    pyspark_file = [result for result in results if result["file_path"] == "tests/test_files/some_pyspark.py"][0]
    csv_file = [result for result in results if result["file_path"] == "tests/test_files/good_schema1.csv"][0]

    assert pyspark_file["word_count"]["rdd"] == 5
    assert pyspark_file["word_count"]["sparkContext"] == 2
    assert csv_file["word_count"]["rdd"] == 0


def test_keyword_format():
    print(keyword_format("spark rdd stuff"))
    print(keyword_format("spark rdd stuff with bad _jvm"))
    print(keyword_format("nice string"))
    print(keyword_format(""))


def test_surround_substring():

    assert "spark **rdd|| stuff" == surround_substring("spark rdd stuff", "rdd", "**", "||")
    assert "spark **rdd|| stuff with **rdd||" == surround_substring("spark rdd stuff with rdd", "rdd", "**", "||")
    assert "spark **rdd||dd stuff" == surround_substring("spark rdddd stuff", "rdd", "**", "||")
