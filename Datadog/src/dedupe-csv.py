# TBD

import os
import csv
import datetime
import importlib
from numpy import average
from dotenv import load_dotenv

load_dotenv()

def epoch_to_datetime(epoch_time_secs):
    # using the datetime.fromtimestamp() function
    date_time = datetime.datetime.fromtimestamp(epoch_time_secs)
    # print("Given epoch time:", epoch_time_secs)
    # print("Converted Datetime:", date_time)
    return date_time


def write_dict_to_csv(file_name, dict_data, csv_columns):
    data_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", file_name)
    )
    print("writing to " + data_file)
    try:
        with open(data_file, "w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=csv_columns,
                delimiter=",",
                quotechar='"',
                lineterminator="\n",
                quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("I/O error")


def read_from_file(file_name):
    data_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", file_name)
    )
    print("reading from " + data_file)
    with open(data_file, mode="r") as f:
        a = [
            {k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)
        ]
        # print(a)
        return a


def max_min_checker(a, b, use_max):
    if a == " " and b == " ":
        return " "
    elif a == " ":
        return b
    elif b == " ":
        return a
    else:
        if use_max:
            return str(max(float(a), float(b)))
        else:
            return str(min(float(a), float(b)))


def get_len_max_min_avg(pointlist, use_max):
    p_builder = []
    for p in pointlist:
        if not (p == " "):
            p_builder.append(float(p))
    if use_max == True:
        return [len(p_builder), max(p_builder), average(p_builder)]
    else:
        return [len(p_builder), min(p_builder), average(p_builder)]


def clean_data(instance_identifier, d, prefix, use_max, num_tags, num_datapoints):
    instanceidentifier_list = {}
    for row in d:
        current_key = row[instance_identifier]
        if current_key in instanceidentifier_list:
            # print(current_key + " exists")
            existing_entry = instanceidentifier_list[current_key]
            if len(row["scope"]) > len(existing_entry["scope"]):
                # print("assume new row is the newer entry")
                # print("prev scope " + existing_entry["scope"])
                existing_entry["scope"] = row["scope"]
                # print("new scope " + existing_entry["scope"])

                # pick max/min values
                points_list = []
                for x in range(1, num_datapoints + 1):
                    existing_entry[prefix + "_d" + str(x)] = max_min_checker(
                        existing_entry[prefix + "_d" + str(x)],
                        row[prefix + "_d" + str(x)],
                        use_max,
                    )
                    points_list.append(existing_entry[prefix + "_d" + str(x)])
                # determine new count,max/min,avg
                len_max_min_avg = get_len_max_min_avg(points_list, use_max)
                existing_entry[prefix + "_count"] = len_max_min_avg[0]
                existing_entry[
                    prefix + "_" + ("max" if use_max == True else "min")
                ] = len_max_min_avg[1]
                existing_entry[prefix + "_avg"] = len_max_min_avg[2]

                for x in range(1, num_tags + 1):
                    existing_entry["tags_" + str(x)] = row["tags_" + str(x)]

            else:
                # print("assume existing row is the newer entry")
                # print("\t existing:" + existing_entry["scope"])
                # print("\t current:" + row["scope"])

                # pick max/min values
                points_list = []
                for x in range(1, num_datapoints + 1):
                    existing_entry[prefix + "_d" + str(x)] = max_min_checker(
                        existing_entry[prefix + "_d" + str(x)],
                        row[prefix + "_d" + str(x)],
                        use_max,
                    )
                    points_list.append(existing_entry[prefix + "_d" + str(x)])
                # determine new count,max,avg
                len_max_min_avg = get_len_max_min_avg(points_list, use_max)
                existing_entry[prefix + "_count"] = len_max_min_avg[0]
                existing_entry[
                    prefix + "_" + ("max" if use_max == True else "min")
                ] = len_max_min_avg[1]
                existing_entry[prefix + "_avg"] = len_max_min_avg[2]

        else:
            instanceidentifier_list[current_key] = row
    # print(dbinstanceidentifier_list)
    return instanceidentifier_list

file_name_mgr = importlib.import_module("file-name-mgr-ec")
for x in range(0, 5):
    # x = 3
    dict = read_from_file(file_name_mgr.file_name(x, ".csv"))
    if file_name_mgr.agg(x) == "max":
        clean_dict = clean_data(
            "cacheclusterid", dict, file_name_mgr.prefix(x), True, 6, 14
        )
    else:
        clean_dict = clean_data(
            "cacheclusterid", dict, file_name_mgr.prefix(x), False, 6, 14
        )

    use_max = file_name_mgr.agg(x) == "max"
    write_dict_to_csv(
        file_name_mgr.file_name(x, "-new.csv"),
        list(clean_dict.values()),
        file_name_mgr.get_csv_columns(file_name_mgr.prefix(x), use_max, 6, 14),
    )

# read_util_data(fn[x] + ".txt", fn[x] + ".csv", pref[x])
# data_file = os.path.join(os.path.dirname(__file__), "..", "data", "test1.csv")
# write_to_csv(data_file, ['test', 'test 3'])
