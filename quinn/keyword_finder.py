from __future__ import annotations

import os
from dataclasses import dataclass
from glob import iglob

default_keywords = [
    "_jsc",
    "_jconf",
    "_jvm",
    "_jsparkSession",
    "_jreader",
    "_jc",
    "_jseq",
    "_jdf",
    "_jmap",
    "_jco",
    "emptyRDD",
    "range",
    "init_batched_serializer",
    "parallelize",
    "pickleFile",
    "textFile",
    "wholeTextFiles",
    "binaryFiles",
    "binaryRecords",
    "sequenceFile",
    "newAPIHadoopFile",
    "newAPIHadoopRDD",
    "hadoopFile",
    "hadoopRDD",
    "union",
    "runJob",
    "setSystemProperty",
    "uiWebUrl",
    "stop",
    "setJobGroup",
    "setLocalProperty",
    "getCon",
    "rdd",
    "sparkContext",
]

@dataclass
class SearchResult:
    """Class to hold the results of a file search.
    file_path: The path to the file that was searched.
    word_count: A dictionary containing the number of times each keyword was found in the file.
    """

    file_path: str
    word_count: dict[str, int]


def search_file(path: str, keywords: list[str] = default_keywords) -> SearchResult:
    """Searches a file for keywords and prints the line number and line containing the keyword.

    :param path: The path to the file to search.
    :type path: str
    :param keywords: The list of keywords to search for.
    :type keywords: list[str]
    :returns: A dictionary containing a file path and the number of lines containing a keyword in `keywords`.
    :rtype: SearchResult

    """
    match_results = SearchResult(file_path=path, word_count={keyword: 0 for keyword in keywords})

    print(f"\nSearching: {path}")
    with open(path) as f:
        for line_number, line in enumerate(f, 1):
            line_printed = False
            for keyword in keywords:
                if keyword in line:
                    match_results.word_count[keyword] += 1

                    if not line_printed:
                        print(f"{line_number}: {keyword_format(line)}", end="")
                        line_printed = True

    return match_results


def search_files(path: str, keywords: list[str] = default_keywords) -> list[SearchResult]:
    """Searches all files in a directory for keywords.

    :param path: The path to the directory to search.
    :type path: str
    :param keywords: The list of keywords to search for.
    :type keywords: list[str]
    :returns: A list of dictionaries containing file paths and the number of lines containing a keyword in `keywords`.
    :rtype: list[SearchResult]

    """
    rootdir_glob = f"{path}/**/*"
    file_list = [f for f in iglob(rootdir_glob, recursive=True) if os.path.isfile(f)]
    return [search_file(f, keywords) for f in file_list]


def keyword_format(input: str, keywords: list[str] = default_keywords) -> str:
    """Formats the input string to highlight the keywords.

    :param input: The string to format.
    :type input: str
    :param keywords: The list of keywords to highlight.
    :type keywords: list[str]

    """
    nc = "\033[0m"
    red = "\033[31m"
    bold = "\033[1m"
    res = input
    for keyword in keywords:
        res = surround_substring(res, keyword, red + bold, nc)
    return res


def surround_substring(input: str, substring: str, surround_start: str, surround_end: str) -> str:
    """Surrounds a substring with the given start and end strings.

    :param input: The string to search.
    :type input: str
    :param substring: The substring to surround.
    :type substring: str
    :param surround_start: The string to start the surrounding with.
    :type surround_start: str
    :param surround_end: The string to end the surrounding with.
    :type surround_end: str
    :returns: The input string with the substring surrounded.
    :rtype: str

    """
    return input.replace(
        substring,
        surround_start + substring + surround_end,
    )
