import os
import json
import csv
import datetime

import myUtils
import fileNameMgrContainer

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

def read_util_data(
    file_name, output_file_name, prefix, use_max, num_tags, num_datapoints
):
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
        write_to_csv(
            output_data_file,
            ["", "", prefix + "_" + ("max" if use_max == True else "min")],
        )

    # write_headers() #comment out if not writing headers

    write_to_csv(
        output_data_file,
        fileNameMgrContainer.get_csv_headers(prefix, use_max, num_tags, num_datapoints),
    )

    for series_obj in json_object["series"]:  # array of Json
        arr_builder = []
        tag_set = series_obj["tag_set"]
        if "pod_name:" in tag_set[0]:
            arr_builder.append(tag_set[0].replace("pod_name:", ""))
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
        if use_max == True:
            arr_builder.append(max(p_builder))
        else:
            arr_builder.append(min(p_builder))

        arr_builder.append(average(p_builder))

        for p in pointlist:
            if p[1] is None:
                arr_builder.append(" ")
            else:
                arr_builder.append(p[1])

        for t in tag_set:
            arr_builder.append(t)
        write_to_csv(output_data_file, arr_builder)


class outputRow:
    scope = '' #identifier
    query = ''
    metricsHolder = [
        {'metricName':"memUtil",'count':1,'max':2,'avg':2,'data':[]}
    ]
    tags = {
        "key1": "value1",
        "k2": "v2"
    }

def read_data_to_collector(
    file_name, collector, prefix, use_max, num_tags, num_datapoints
):
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

    # collector #dict of format {'scope': outputRow}
    for series_obj in json_object["series"]:  # array of Json        
        scope = json_object["scope"] # unique id
        outputRowObj
        if scope in collector:
            outputRowObj = collector[scope]
        else:
            outputRowObj = outputRow() # new outputRow

        
        arr_builder = []
        tag_set = series_obj["tag_set"]

        arr_builder.append(series_obj["scope"])

        # determine count,max,avg of datadog record
        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        arr_builder.append(len(p_builder))
        if use_max == True:
            arr_builder.append(max(p_builder))
        else:
            arr_builder.append(min(p_builder))

        arr_builder.append(average(p_builder))

        for p in pointlist:
            if p[1] is None:
                arr_builder.append(" ")
            else:
                arr_builder.append(p[1])

        for t in tag_set:
            arr_builder.append(t)
            

        
    outputRowObj = outputRow()
    outputRowObj.scope = 
    # for each item in series
    query = json_object["query"]


    thisdict["color"] = "red"

         ["group_by", json_object["group_by"]])
        f = epoch_to_datetime(json_object["from_date"] / 1000)
        t = epoch_to_datetime(json_object["to_date"] / 1000)
        write_to_csv(
            output_data_file,
            ["period", "from", f, "to", t],
        )
        write_to_csv(
            output_data_file,
            ["", "", prefix + "_" + ("max" if use_max == True else "min")],
        )

    # write_headers() #comment out if not writing headers


def collate_util_data(timeRange, clusterIndex=0):
    outputCollector = {}
    # get mem util - should push mem data to outputCollector of type {'scope': outputRow}

    fileNameTxt_memUtil = fileNameMgrContainer.file_name(0, clusterIndex, timeRange, "txt")
    print("reading " + fileNameTxt_memUtil)
    read_util_data()

    # get mem limit
    fileNameTxt_memLimit = fileNameMgrContainer.file_name(1, clusterIndex, timeRange, "txt")

    # get cpu req
    fileNameTxt_cpuReq = fileNameMgrContainer.file_name(2, clusterIndex, timeRange, "txt")

    # get cpu limit
    fileNameTxt_cpuLimit = fileNameMgrContainer.file_name(3, clusterIndex, timeRange, "txt")

    # calculate cpu util

    # output to csv using outputCollector


for x in range(0, 1):
    timeRange = myUtils.getTimeRange(1,14)
    save_file_name_csv = fileNameMgrContainer.file_name(x, 0, timeRange, "csv")
    print("saving " + save_file_name_csv)
    file_name_txt = fileNameMgrContainer.file_name(x, 0, timeRange, "txt")
    prefix = fileNameMgrContainer.getPrefix(x)
    use_max = fileNameMgrContainer.getAgg(x) == "max"
    read_util_data(file_name_txt, save_file_name_csv, prefix, use_max, num_tags=6, num_datapoints = 84)

# data_file = os.path.join(os.path.dirname(__file__), "..", "data", "test1.csv")
# write_to_csv(data_file, ['test', 'test 3'])
