
import myUtils
import env

arr = [
    [0, "max:kubernetes.memory.usage_pct", None, "max", "memutil","",".rollup(avg,14400)"],
    [0, "max:kubernetes.memory.limits", None, "max", "memlimit","",".rollup(avg,14400)"],
    [0, "max:kubernetes.cpu.usage.total", None, "max", "cpuuse","",".rollup(avg,14400)"],
    [0, "max:kubernetes.cpu.requests", None, "max", "cpulimit","",".rollup(avg,14400)"]
]

kube_cluster_names = env.kube_cluster_names

dateTimeVar = myUtils.getTimeRange(14)

def dd_metric_name(x):
    return arr[x][1]

def as_rate_or_not(x):
    return arr[x][5]

def rollup(x):
    return arr[x][6]

def getAgg(x):
    return arr[x][3]

def file_name(metricIndex, clusterIndex, timeRange, ext):        
    fileOutputName = [
        "max.kubernetes.memory.usage_pct - "
        + kube_cluster_names[clusterIndex]
        + " - "
        + timeRange
        + "." + ext,
        "max.kubernetes.memory.limits - "
        + kube_cluster_names[clusterIndex]
        + " - "
        + timeRange
        + "." + ext,
        "max.kubernetes.cpu.usage.total - "
        + kube_cluster_names[clusterIndex]
        + " - "
        + timeRange
        + "." + ext,
        "max.kubernetes.cpu.requests - "
        + kube_cluster_names[clusterIndex]
        + " - "
        + timeRange
        + "." + ext,
    ]
    return fileOutputName[metricIndex]

def athena_table_name(x):
    return (
        "dd-k8-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
    ).replace("-", "_")
def s3_key(x, ext):
    return (
        "dd-k8-"
        + (
            arr[x][2]
            if arr[x][2] is not None
            else arr[x][1].replace("_", "-")
        )
        + "-"
        + arr[x][3]
        + "/"
        + file_name(x, ext)
    )
def s3_folder(x):
    return (
        "dd-k8-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "/"
    )
def getPrefix(x): 
    return arr[x][4]

def get_csv_columns(prefix, use_max, num_tags, num_datapoints):
    cols = [
        "pod_name",
        "scope",
        prefix + "_count",
        prefix + "_" + ("max" if use_max == True else "min"),
        prefix + "_avg",
    ]
    for d in range(1, num_datapoints + 1):
        cols.append(prefix + "_d" + str(d))
    for t in range(1, num_tags + 1):
        cols.append("tags_" + str(t))
    return cols