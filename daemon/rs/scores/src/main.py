from os import listdir
from os.path import isfile, isdir, join
import sys
import json

name_to_uid = {
    'WordCount-wikipedia-44g': 1,
    'WordCount-wikipedia-88g': 8,
    'pagerank-1m': 2,
    'cc-5m': 3,
    'als': 4,
    'kmeans-points_5.txt': 5,
    'pagerank-5m': 6,
    'kmeans-points_4.txt': 7
}

def get_score_from_summary_log(fname):
    name = fname
    if name[-3:] == ".js":
        name = name[:-3]
    lst = name.split("/")[-1].split("-")
    uid = name_to_uid["-".join(lst[6:])]

    f = open(fname, "r")
    j = json.loads(f.read())
    emc = j["conf_id"].replace("-", "")
    score = j["running_time"]
    f.close()

    return uid, emc, score

# Creates score file with name:
# "[uid]_scores.txt"
def main(path):
    # string: a directory containing log directories.
    fnames = {}
    for i in range(1,9):
        fname = str(i) + "_scores.txt"
        fnames[i] = open(fname, "w")
    for d in listdir(path):
        pd = join(path, d)
        summary_log = d + ".js"
        if summary_log in listdir(pd):
            print summary_log
            uid, emc, score = get_score_from_summary_log(join(pd, summary_log))
            fnames[uid].write(",".join([str(uid), str(emc), str(score)]) + "\n")
    for i in range(1,9):
        fnames[i].close()


if __name__ == "__main__":
    main(sys.argv[1])
