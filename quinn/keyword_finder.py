import os
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
    "_jco"
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


def search_file(path, keywords=default_keywords):
    print(f"\nSearching: {path}")
    with open(path) as f:
        for line_number, line in enumerate(f, 1):
            for keyword in keywords:    
                if keyword in line:
                    print(f"{line_number}: {keyword_format(line)}", end='')
                    break


def search_files(path, keywords=default_keywords):
    rootdir_glob = f"{path}/**/*"
    file_list = [f for f in iglob(rootdir_glob, recursive=True) if os.path.isfile(f)]
    for f in file_list:
        search_file(f)


def keyword_format(input, keywords=default_keywords):
    nc = '\033[0m'
    red = '\033[31m'
    bold = '\033[1m'
    res = input
    for keyword in keywords:
        res = surround_substring(res, keyword, red+bold, nc)
    return res


def surround_substring(input, substring, surround_start, surround_end):
    index = input.find(substring)
    res = ""
    if index == -1:
        res = input
    else:
        res = input[:index] + surround_start + substring + surround_end + input[(index+len(substring)):]
    return res
