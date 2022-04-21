# key metrics: https://aws.amazon.com/premiumsupport/knowledge-center/opensearch-high-sysmemoryutilization/

arr = [
    [0, "cpuutilization", None, "max", "cpuutil","",".rollup(max,86400)"],
    [1, "jvmmemory_pressure", None, "max", "jvmmemory_pressure","",".rollup(max,86400)"],
    [2, "free_storage_space", None, "min", "free_storage_space","",".rollup(min,86400)"]
]

def dd_metric_name(x):
    return arr[x][1]

def as_rate_or_not(x):
    return arr[x][5]

def rollup(x):
    return arr[x][6]

def agg(x):
    return arr[x][3]

def file_name(x, ext):
    return (
        "dd-os-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "-20220401-20220415" + ext
    )
def athena_table_name(x):
    return (
        "dd-os-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
    ).replace("-", "_")
def s3_key(x, ext):
    return (
        "dd-os-"
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
        "dd-os-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "/"
    )
def prefix(x): 
    return arr[x][4]

def get_csv_columns(prefix, use_max, num_tags, num_datapoints):
    cols = [
        "domainname",
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