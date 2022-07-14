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


def read_util_data(file_name, output_file_name, prefix, use_max, num_tags, num_datapoints):
    data_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", file_name))
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
    output_data_file = os.path.join(os.path.dirname(__file__), "..", "data", output_file_name)

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
        fileNameMgrContainer.get_csv_columns(prefix, use_max, num_tags, num_datapoints),
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


class outputRowSeriesItem:
    metricName = ""
    aggr = ""
    expression = ""
    tag_set = []
    startTime = None
    endTime = None
    interval = None
    rawDataLength = None
    metricCount = None
    metricMax = None
    metricMin = None
    metricAvg = None
    # metricsRawData = []
    unit = []

    def getUnitScaling(self):
        # print(self.unit)
        if len(self.unit) > 0:
            return self.unit[0]["scale_factor"]
        else:
            return None

    def __init__(self, metricName):
        self.metricName = metricName
        self.tag_set = []
        self.unit = []

    def __repr__(self):
        return (
            "\t- "
            + self.metricName
            + "\tcount: "
            + str(self.metricCount)
            + ", max: "
            + str(self.metricMax)
            + ", avg: "
            + str(self.metricAvg)
            + ", min: "
            + str(self.metricMin)
            + ", scale_factor: "
            + str(self.getUnitScaling())
            + "\n"
        )


class outputRow:
    scope = ""  # identifier
    seriesItems = []  # class: outputRowSeriesItem

    def __init__(self, scope_):
        self.scope = scope_
        self.seriesItems = []

    def __repr__(self):
        resp = ""
        for s in self.seriesItems:
            resp = resp + s.__repr__()
        return "\nseriesItems length: " + str(len(self.seriesItems)) + ": \n" + resp
        # return "scope: " + self.scope + " seriesItems length: " + str(len(self.seriesItems)) + ": \n" + resp

    def populate_cpu_util(self):
        cpuUseMax = None
        cpuUseAvg = None
        cpuReqMax = None
        cpuReqAvg = None
        for s in self.seriesItems:
            print(s.metricName)
            if s.metricName == "kubernetes.cpu.usage.total":
                scaling = s.getUnitScaling()
                cpuUseMax = s.metricMax * scaling
                cpuUseAvg = s.metricAvg * scaling
            elif s.metricName == "kubernetes.cpu.requests":
                scaling = s.getUnitScaling()
                cpuReqMax = s.metricMax * scaling
                cpuReqAvg = s.metricAvg * scaling
                # print(scaling)
                # print(cpuReqMax)
                # print(cpuReqAvg)

        cpuOutputRowSI = outputRowSeriesItem("max.kubernetes.cpu.usage_pct")
        cpuOutputRowSI.metricMax = cpuUseMax / cpuReqMax
        cpuOutputRowSI.metricAvg = cpuUseAvg / cpuReqAvg

        self.seriesItems.append(cpuOutputRowSI)

        return {"cpuUseMax": cpuUseMax, "cpuUseAvg": cpuUseAvg, "cpuReqMax": cpuReqMax, "cpuReqAvg": cpuReqAvg}


def read_data_to_collector(file_name, collector, prefix="", use_max=True):
    data_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", file_name))
    input_str = read_from_file(data_file)
    # print(input_str)

    # Clean input_str
    input_str = str(input_str).replace("'\n                           '", "")
    input_str = str(input_str).replace("'\n          '", "")
    input_str = str(input_str).replace("'short_name': None", "'short_name': 'None'")
    input_str = str(input_str).replace("'unit': None", "'unit': 'None'")
    input_str = str(input_str).replace("'", '"')
    input_str = str(input_str).replace(" None]", " null]")
    json_object = json.loads(input_str)
    # print(type(json_object))

    # loop through series
    # collector is dict of format {'scope': class:outputRow }
    for series_obj in json_object["series"]:
        scope = series_obj["scope"]  # unique id
        # print("scope: " + scope)
        if scope in collector:
            outputRowObj = collector[scope]
            # print("existing row")
        else:
            outputRowObj = outputRow(scope)  # new outputRow
            # print(outputRowObj)
            # print("new outputRowObj")

        metricName = series_obj["display_name"]  # 'kubernetes.memory.usage_pct'
        outputRowSI = outputRowSeriesItem(metricName)
        outputRowSI.aggr = series_obj["aggr"]  # 'max'
        outputRowSI.expression = series_obj["expression"]  # 'max:kubernetes.memory.usage_pct{kube_app_managed_by:xxx,kube_cluster_name:xxx,kube_namespace:xxx}.rollup(avg, '14400')
        outputRowSI.tag_set = series_obj["tag_set"]
        outputRowSI.rawDataLength = series_obj["length"]
        outputRowSI.startTime = series_obj["start"]
        outputRowSI.endTime = series_obj["end"]
        outputRowSI.interval = series_obj["interval"]
        outputRowSI.unit = series_obj["unit"]

        # determine count,max,avg of record
        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        outputRowSI.metricCount = len(p_builder)
        outputRowSI.metricMax = max(p_builder)
        outputRowSI.metricMin = min(p_builder)
        outputRowSI.metricAvg = average(p_builder)

        outputRowObj.seriesItems.append(outputRowSI)
        print(outputRowObj)

        collector[scope] = outputRowObj

    return collector


def populate_cpu_util(collector):
    for v in collector.values():
        v.populate_cpu_util()
    return collector
    # get max.kubernetes.cpu.usage.total
    # get max.kubernetes.cpu.requests


def collate_util_data(timeRange, clusterIndex=0):
    outputCollector = {}
    # get mem util - should push mem data to outputCollector of type {'scope': outputRow}

    fileNameTxt_memUtil = fileNameMgrContainer.file_name(0, clusterIndex, timeRange, "txt")
    fileNameTxt_memUtil = "max.kubernetes.memory.usage_pct - staging-sample - 20220629-20220713.txt"  # test
    print("reading " + fileNameTxt_memUtil)
    outputCollector = read_data_to_collector(fileNameTxt_memUtil, outputCollector)
    # print(outputCollector)

    # get mem limit
    fileNameTxt_memLimit = fileNameMgrContainer.file_name(1, clusterIndex, timeRange, "txt")
    fileNameTxt_memLimit = "max.kubernetes.memory.limits - staging-sample - 20220629-20220713.txt"  # test
    outputCollector = read_data_to_collector(fileNameTxt_memLimit, outputCollector)

    # get cpu req
    fileNameTxt_cpuReq = fileNameMgrContainer.file_name(2, clusterIndex, timeRange, "txt")
    fileNameTxt_cpuReq = "max.kubernetes.cpu.usage.total - staging-sample - 20220629-20220713.txt"  # test
    outputCollector = read_data_to_collector(fileNameTxt_cpuReq, outputCollector)
    # get cpu limit
    fileNameTxt_cpuLimit = fileNameMgrContainer.file_name(3, clusterIndex, timeRange, "txt")
    fileNameTxt_cpuLimit = "max.kubernetes.cpu.requests - staging-sample - 20220629-20220713.txt"  # test
    outputCollector = read_data_to_collector(fileNameTxt_cpuLimit, outputCollector)

    # calculate cpu util
    outputCollector = populate_cpu_util(outputCollector)

    # output to csv using outputCollector
    print(outputCollector)


timeRange = myUtils.getTimeRange(1, 14)
collate_util_data(timeRange)

# for x in range(0, 1):
#     timeRange = myUtils.getTimeRange(1,14)
#     save_file_name_csv = fileNameMgrContainer.file_name(x, 0, timeRange, "csv")
#     print("saving " + save_file_name_csv)
#     file_name_txt = fileNameMgrContainer.file_name(x, 0, timeRange, "txt")
#     prefix = fileNameMgrContainer.getPrefix(x)
#     use_max = fileNameMgrContainer.getAgg(x) == "max"
#     read_util_data(file_name_txt, save_file_name_csv, prefix, use_max, num_tags=6, num_datapoints = 84)

# data_file = os.path.join(os.path.dirname(__file__), "..", "data", "test1.csv")
# write_to_csv(data_file, ['test', 'test 3'])
