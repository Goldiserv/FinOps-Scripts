import os
import json
import csv

# import datetime

# import myUtils
import fileNameMgrContainer

from numpy import average
from dotenv import load_dotenv

load_dotenv()


def append_csv_row(data_file, data_row_array):
    with open(data_file, "a", newline="") as csvfile:
        csv_w = csv.writer(csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        csv_w.writerow(data_row_array)


def write_csv_row(data_file, data_row_array):
    with open(data_file, "w", newline="") as csvfile:
        csv_w = csv.writer(csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        csv_w.writerow(data_row_array)
        csvfile.close()


def read_from_file(path):
    with open(path, "r") as o:
        resp = o.read()
        o.close()
        return resp


def get_csv_headers_v2(tagKeys):
    headers = []
    # Add tag names
    headers.extend(tagKeys)

    # Add scope
    headers.append("scope")

    # Add metrics
    # headers.extend(["mem_pct_metric", "mem_pct_expression", "mem_pct_count", "mem_pct_max", "mem_pct_avg"])
    # headers.extend(["mem_limit_metric", "mem_limit_expression", "mem_limit_count", "mem_limit_max", "mem_limit_avg"])
    # headers.extend(["cpu_pct_metric", "cpu_pct_expression", "cpu_pct_count", "cpu_pct_max", "cpu_pct_avg"])
    # headers.extend(["cpu_limit_metric", "cpu_limit_expression", "cpu_limit_count", "cpu_limit_max", "cpu_limit_avg"])
    headers.extend(["mem_pct_metric", "mem_pct_count", "mem_pct_max", "mem_pct_avg"])
    headers.extend(["mem_limit_metric", "mem_limit_count", "mem_limit_max", "mem_limit_avg"])
    headers.extend(["cpu_pct_metric", "cpu_pct_count", "cpu_pct_max", "cpu_pct_avg"])
    headers.extend(["cpu_limit_metric", "cpu_limit_count", "cpu_limit_max", "cpu_limit_avg"])

    return headers


class outputRowSeriesItem:
    metricName = None
    aggr = None
    expression = None
    tag_set = []
    tag_set_dict = {}
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
        self.tag_set_dict = {}

    def getTagKeys(self):
        return self.tag_set_dict.keys()

    def createTagSetDict(self):
        for tagKV in self.tag_set:
            tagK = tagKV.split(":")[0]
            tagV = tagKV[(len(tagK) + 1) :]
            if tagK in self.tag_set_dict:
                self.tag_set_dict[tagK].append(tagV)
            else:
                self.tag_set_dict[tagK] = [tagV]

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

    def get_csv_row(self):
        return [
            self.metricName,
            # self.expression,
            self.metricCount,
            self.metricMax,
            self.metricAvg,
        ]


class outputRow:
    scope = ""  # identifier
    seriesItems = []  # class: outputRowSeriesItem

    def get_metric_names(self):
        metricNameList = []
        for s in self.seriesItems:
            metricNameList.append(s.metricName)
        return metricNameList

    def get_csv_row(self, metricName):
        for s in self.seriesItems:
            if s.metricName == metricName:
                return s.get_csv_row()
        return ["-", "-", "-", "-"]

    def get_tag_keys(self):
        tagKeyHolder = []
        for i in self.seriesItems:
            tagKeyHolder.extend(i.tag_set_dict.keys())
        return set(tagKeyHolder)

    def get_tag_values(self, tagK):
        # only return first of series item tag
        return self.seriesItems[0].tag_set_dict[tagK]

    def get_tags(self):
        if len(self.seriesItems) > 0:
            return self.seriesItems[0].tag_set
        else:
            return ["empty seriesItems"]

    def __init__(self, scope):
        self.scope = scope
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
            # print(s.metricName)
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

        cpuOutputRowSI = outputRowSeriesItem("kubernetes.cpu.usage_pct")
        cpuOutputRowSI.aggr = "max"
        try:
            cpuOutputRowSI.metricMax = cpuUseMax / cpuReqMax
        except:
            # print("An exception occurred with cpuOutputRowSI.metricMax:" + "\nscope: " + self.scope + "\ncpuUseMax: " + str(cpuUseMax) + "\ncpuReqMax: " + str(cpuReqMax))
            cpuOutputRowSI.metricMax = "-"

        try:
            cpuOutputRowSI.metricAvg = cpuUseAvg / cpuReqAvg
        except:
            # print("An exception occurred with cpuOutputRowSI.metricAvg" + "\nscope: " + self.scope + "\ncpuUseMax: " + str(cpuUseAvg) + "\ncpuReqMax: " + str(cpuReqAvg))
            cpuOutputRowSI.metricAvg = "-"

        self.seriesItems.append(cpuOutputRowSI)

        return {"cpuUseMax": cpuUseMax, "cpuUseAvg": cpuUseAvg, "cpuReqMax": cpuReqMax, "cpuReqAvg": cpuReqAvg}


def write_output(output_file_name, collector, tagKeys, prefix="", use_max=True):
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", output_file_name)

    # write metadata
    # from-to, data sources, etc.

    # write header array
    headerArr = get_csv_headers_v2(tagKeys)
    write_csv_row(out_dir, headerArr)

    # write rows
    for outputRow in collector.values():
        dataCollector = []
        # only write tags from first series item in
        for tagK in tagKeys:
            tagV = outputRow.get_tag_values(tagK)
            # print(tagV)
            dataCollector.append(tagV)

        dataCollector.append(outputRow.scope)

        try:
            dataCollector.extend(outputRow.get_csv_row("kubernetes.memory.usage_pct"))
        except:
            # print("1. available metric names: " + str(outputRow.get_metric_names()))
            pass

        try:
            dataCollector.extend(outputRow.get_csv_row("kubernetes.memory.limits"))
        except:
            # print("2. available metric names: " + str(outputRow.get_metric_names()))
            pass

        try:
            dataCollector.extend(outputRow.get_csv_row("kubernetes.cpu.usage_pct"))
        except:
            # print("3. available metric names: " + str(outputRow.get_metric_names()))
            pass

        try:
            dataCollector.extend(outputRow.get_csv_row("kubernetes.cpu.requests"))
        except:
            # print("4. available metric names: " + str(outputRow.get_metric_names()))
            pass

        append_csv_row(out_dir, dataCollector)

    # print(dataCollector)


def read_data_to_collector(file_name, collector, tagKeys, minNumPoints=0):
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

        pointlist = series_obj["pointlist"]
        p_builder = []
        for p in pointlist:
            if not (p[1] is None):
                p_builder.append(p[1])
        countNumPoints = len(p_builder)

        if countNumPoints >= minNumPoints:
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
            outputRowSI.expression = series_obj[
                "expression"
            ]  # 'max:kubernetes.memory.usage_pct{kube_app_managed_by:xxx,kube_cluster_name:xxx,kube_namespace:xxx}.rollup(avg, '14400')
            outputRowSI.tag_set = series_obj["tag_set"]
            outputRowSI.createTagSetDict()
            tagKeys.extend(outputRowSI.getTagKeys())
            # print(outputRowSI.tag_set_dict)
            outputRowSI.rawDataLength = series_obj["length"]
            outputRowSI.startTime = series_obj["start"]
            outputRowSI.endTime = series_obj["end"]
            outputRowSI.interval = series_obj["interval"]
            outputRowSI.unit = series_obj["unit"]

            # determine count,max,avg of record
            outputRowSI.metricCount = countNumPoints
            outputRowSI.metricMax = max(p_builder)
            outputRowSI.metricMin = min(p_builder)
            outputRowSI.metricAvg = average(p_builder)

            outputRowObj.seriesItems.append(outputRowSI)
            # print(outputRowObj)

            collector[scope] = outputRowObj

    return collector


def populate_cpu_util(collector):
    for v in collector.values():
        v.populate_cpu_util()
    return collector


def collate_util_data(timeRange, clusterIndex=0, testingMode=True, minNumPoints=0):
    outputCollector = {}
    # get mem util - should push mem data to outputCollector of type {'scope': outputRow}
    tagKeys = []
    

    print("collating util data for: " + (fileNameMgrContainer.kube_cluster_names[clusterIndex] if not testingMode else "staging-sample"))

    fileNameTxt_memUtil = (
        fileNameMgrContainer.file_name(0, clusterIndex, timeRange, "txt") if not testingMode else "max.kubernetes.memory.usage_pct - staging-sample - 20220629-20220713.txt"
    )
    # print("reading " + fileNameTxt_memUtil)
    outputCollector = read_data_to_collector(fileNameTxt_memUtil, outputCollector, tagKeys, minNumPoints)
    # print(outputCollector)

    # get mem limit
    fileNameTxt_memLimit = (
        fileNameMgrContainer.file_name(1, clusterIndex, timeRange, "txt") if not testingMode else "max.kubernetes.memory.limits - staging-sample - 20220629-20220713.txt"
    )
    outputCollector = read_data_to_collector(fileNameTxt_memLimit, outputCollector, tagKeys, minNumPoints)

    # get cpu req
    fileNameTxt_cpuReq = (
        fileNameMgrContainer.file_name(2, clusterIndex, timeRange, "txt") if not testingMode else "max.kubernetes.cpu.usage.total - staging-sample - 20220629-20220713.txt"
    )
    outputCollector = read_data_to_collector(fileNameTxt_cpuReq, outputCollector, tagKeys, minNumPoints)
    # get cpu limit
    fileNameTxt_cpuLimit = (
        fileNameMgrContainer.file_name(3, clusterIndex, timeRange, "txt") if not testingMode else "max.kubernetes.cpu.requests - staging-sample - 20220629-20220713.txt"
    )
    outputCollector = read_data_to_collector(fileNameTxt_cpuLimit, outputCollector, tagKeys, minNumPoints)

    # clean tagKeys
    tagKeys = list(set(tagKeys))
    tagKeys.sort()
    # print(tagKeys)
    # e.g. ['kube_cluster_name', 'kube_ownerref_name', 'kube_app_managed_by', 'kube_namespace', 'kube_service', 'pod_name']

    # calculate cpu util
    outputCollector = populate_cpu_util(outputCollector)
    # print(outputCollector)

    # # output to csv using outputCollector
    save_file_name_csv = fileNameMgrContainer.file_name(4, clusterIndex, timeRange, "csv") if not testingMode else "output - staging-sample - 20220629-20220713.csv"
    write_output(save_file_name_csv, outputCollector, tagKeys)


# timeRange = myUtils.getTimeRange(1, 14)
timeRange = "20220629-20220713"
clusterIndex = 3
minNumPoints = 2 # min util points entry should contain before being included
# print(fileNameMgrContainer.kube_cluster_names[clusterIndex])
collate_util_data(timeRange, clusterIndex, False, minNumPoints)

# save_file_name_csv = "output - staging-sample - 20220629-20220713.txt"  # test
# write_output(save_file_name_csv)


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
