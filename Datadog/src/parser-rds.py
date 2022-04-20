# TBD

import os
import json
import csv
import datetime
from numpy import average
from dotenv import load_dotenv

load_dotenv()


def epoch_to_datetime(epoch_time_secs):
    # using the datetime.fromtimestamp() function
    date_time = datetime.datetime.fromtimestamp(epoch_time_secs)
    # print("Given epoch time:", epoch_time_secs)
    # print("Converted Datetime:", date_time)
    return date_time


def write_to_file(data_file, data_str):
    with open(data_file, "a") as o:
        o.write(data_str)
        o.close()


def write_to_csv(data_file, data_row_array):
    with open(data_file, "a", newline="") as csvfile:
        csv_w = csv.writer(csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        # csv_w.writerow(['Spam'] * 5 + ['Baked Beans'])
        csv_w.writerow(data_row_array)


def read_from_file(path):
    with open(path, "r") as o:
        resp = o.read()
        o.close()
        return resp


def read_dummy_data():
    data_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "test.txt")
    )

    # json string data
    input_str = read_from_file(data_file)
    # print(type(input_str))

    # convert string to  object
    json_object = json.loads(input_str)
    # print(type(json_object))

    # access first_name
    for employee in json_object["employees"]:
        print(employee["first_name"])


def read_util_data(file_name, output_file_name, prefix):
    data_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", file_name)
    )
    input_str = read_from_file(data_file)
    # print(input_str)
    input_str = str(input_str).replace("'\n                           '", "")
    input_str = str(input_str).replace("'\n          '", "")
    input_str = str(input_str).replace("'short_name': None", "'short_name': 'None'")
    input_str = str(input_str).replace("'unit': None", "'unit': 'None'")
    input_str = str(input_str).replace("'", '"')
    input_str = str(input_str).replace(" None]", " null]")
    json_object = json.loads(input_str)
    # print(type(json_object))

    # header
    output_data_file = os.path.join(
        os.path.dirname(__file__), "..", "data", output_file_name
    )

    def write_headers():
        write_to_csv(output_data_file, ["query", json_object["query"]])
        write_to_csv(output_data_file, ["group_by", json_object["group_by"]])
        f = epoch_to_datetime(json_object["from_date"] / 1000)
        t = epoch_to_datetime(json_object["to_date"] / 1000)
        write_to_csv(
            output_data_file,
            ["period", "from", f, "to", t],
        )
        write_to_csv(output_data_file, ["", "", prefix + "_max"])

    # write_headers() #comment out if not writing headers

    write_to_csv(
        output_data_file,
        [
            "dbinstanceidentifier",
            "scope",
            prefix + "_count",
            prefix + "_max",
            prefix + "_avg",
            prefix + "_d1",
            prefix + "_d2",
            prefix + "_d3",
            prefix + "_d4",
            prefix + "_d5",
            prefix + "_d6",
            prefix + "_d7",
            prefix + "_d8",
            prefix + "_d9",
            prefix + "_d10",
            prefix + "_d11",
            prefix + "_d12",
            prefix + "_d13",
            prefix + "_d14",
            "tags_1",
            "tags_2",
            "tags_3",
            "tags_4",
            "tags_5",
            "tags_6",
        ],
    )

    for series_obj in json_object["series"]:  # array of Json
        arr_builder = []
        tag_set = series_obj["tag_set"]
        if "dbinstanceidentifier:" in tag_set[0]:
            arr_builder.append(tag_set[0].replace("dbinstanceidentifier:", ""))
        else:
            arr_builder.append("")

        arr_builder.append(series_obj["scope"])

        # determine count,max,avg of datadog record
        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        arr_builder.append(len(p_builder))
        arr_builder.append(max(p_builder))
        arr_builder.append(average(p_builder))

        for p in pointlist:
            if p[1] is None:
                arr_builder.append(" ")
            else:
                arr_builder.append(p[1])

        for t in tag_set:
            arr_builder.append(t)
        write_to_csv(output_data_file, arr_builder)


fn = [
    "dd-rds-cpuutilization-max-20220401-20220415",
    "dd-rds-dbload-non-cpu-max-20220401-20220415",
    "dd-rds-disk-queue-depth-max-20220401-20220415",
    "dd-rds-ebsiobalance-min-20220401-20220415",
]
pref = ["cpuutil","dbload_non_cpu","rds_disk_queue_depth","rds_ebsiobalance"]
x = 0

read_util_data(fn[x] + ".txt", fn[x] + ".csv", pref[x])

# data_file = os.path.join(os.path.dirname(__file__), "..", "data", "test1.csv")
# write_to_csv(data_file, ['test', 'test 3'])
