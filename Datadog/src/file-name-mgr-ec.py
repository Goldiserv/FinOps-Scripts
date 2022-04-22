
arr = [
    [0, "cpuutilization", None, "max", "cpuutil","",".rollup(max,86400)"],
    [1, "database_memory_usage_percentage", None, "max", "database_memory_usage_percentage","",".rollup(max,86400)"],
    [2, "swap_usage", None, "max", "swap_usage","",".rollup(max,86400)"],
    [3, "network_bandwidth_in_allowance_exceeded", None, "max", "network_bandwidth_in_allowance_exceeded","",".rollup(max,86400)"],
    [4, "network_bandwidth_out_allowance_exceeded", None, "max", "network_bandwidth_out_allowance_exceeded","",".rollup(max,86400)"],
    [5, "unused_memory", None, "min", "unused_memory","",".rollup(min,86400)"],
    [6, "freeable_memory", None, "min", "freeable_memory","",".rollup(min,86400)"],
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
        "dd-ec-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "-20220401-20220415" + ext
    )
def athena_table_name(x):
    return (
        "dd-ec-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
    ).replace("-", "_")
def s3_key(x, ext):
    return (
        "dd-ec-"
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
        "dd-ec-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "/"
    )
def prefix(x): 
    return arr[x][4]

def get_csv_columns(prefix, use_max, num_tags, num_datapoints):
    cols = [
        "cacheclusterid",
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