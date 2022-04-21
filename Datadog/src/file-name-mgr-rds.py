
arr = [
    [0, "cpuutilization", None, "max", "cpuutil","",".rollup(max,86400)"],
    [1, "freeable_memory", None, "min", "freeable_memory","",".rollup(max,86400)"],
    [2, "database_connections", None, "max", "db_conns","",".rollup(max,86400)"],
    [3, "free_storage_space", None, "min", "free_storage","",".rollup(max,86400)"],
    [4, "read_latency", None, "max", "read_latency","",".rollup(max,86400)"],
    [5, "write_latency", None, "max", "write_latency","",".rollup(max,86400)"],
    [6, "network_receive_throughput", "nw-in", "max", "nw_in",".as_rate()",".rollup(sum,3600)"],
    [7, "network_transmit_throughput", "nw-out", "max", "nw_out",".as_rate()",".rollup(sum,3600)"],
    [8, "read_iops", None, "max", "read_iops",".as_rate()",".rollup(sum,3600)"],
    [9, "write_iops", None, "max", "write_iops",".as_rate()",".rollup(sum,3600)"],
    [10, "disk_queue_depth", None, "max", "disk_queue_depth","",".rollup(max,86400)"],
    [11, "dbload_non_cpu", None, "max", "dbload_non_cpu","",".rollup(max,86400)"],
    # [12, "ebsiobalance", None, "min", "ebsiobalance",""],
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
        "dd-rds-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "-20220401-20220415" + ext
    )
def athena_table_name(x):
    return (
        "dd-rds-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
    ).replace("-", "_")
def s3_key(x, ext):
    return (
        "dd-rds-"
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
        "dd-rds-"
        + (arr[x][2] if arr[x][2] is not None else arr[x][1].replace("_", "-"))
        + "-"
        + arr[x][3]
        + "/"
    )
def prefix(x): 
    return arr[x][4]

def get_csv_columns(prefix, use_max, num_tags, num_datapoints):
    cols = [
        "dbinstanceidentifier",
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