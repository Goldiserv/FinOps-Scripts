
arr = [
    [0, "max.kubernetes.memory.usage_pct", None, "max", "memutil","",".rollup(max,3600)"]
]

def dd_metric_name(x):
    return arr[x][1]

def as_rate_or_not(x):
    return arr[x][5]

def rollup(x):
    return arr[x][6]

def agg(x):
    return arr[x][3]

def file_name(x, timeRange, ext):
    return (
        "dd-k8-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "-" + timeRange + ext
    )

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
def prefix(x): 
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