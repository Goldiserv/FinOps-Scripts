__version__ = "1.0.6"

"""
Changelog
1.0.6 - Added "Standard" type for sc1 st1
1.0.5 - Volume level err msg now includes available volume data and does not terminate loop across volumes in account, improved volume level try/catch checks when calculating metrics
1.0.4 - added iopsProvisioned to address ebs without 'Iops' key and err catch for each ebs row
1.0.3 - Added logic to price equivalent EBS on gp3
1.0.2 - Major updates to iops and throughput calcs based on https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using_cloudwatch_ebs.html
1.0.1 - Initial version
"""

from datetime import datetime, timedelta
import datetime
import csv
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

periodGlobal = 600
regionName = os.getenv("AWS_DEFAULT_REGION")
# Can't differentiate sc1 and st1 as they return 'VolumeType': 'standard'. Picking the higher cost st1
pricingEbsSyd = {
    "size_gb_month": {
        "gp2": 0.12,
        "gp3": 0.096,
        "io1": 0.138,
        "io2": 0.138,
        "st1": 0.054,
        "sc1": 0.018,
        "standard": 0.054,
    },
    "piop_month_0_to_3000": {
        "gp2": 0.0,
        "gp3": 0.0,
        "io1": 0.072,
        "io2": 0.072,
        "st1": 0.0,
        "sc1": 0.0,
        "standard": 0.0,
    },
    "piop_month_3000_to_32000": {
        "gp2": 0.0,
        "gp3": 0.006,
        "io1": 0.072,
        "io2": 0.072,
        "st1": 0.0,
        "sc1": 0.0,
        "standard": 0.0,
    },
    "piop_month_32000_to_64000": {
        "gp2": 0.0,
        "gp3": 0.006,
        "io1": 0.072,
        "io2": 0.050,
        "st1": 0.0,
        "sc1": 0.0,
        "standard": 0.0,
    },
    "piop_month_over_64000": {
        "gp2": 0.0,
        "gp3": 0.006,
        "io1": 0.072,
        "io2": 0.035,
        "st1": 0.0,
        "sc1": 0.0,
        "standard": 0.0,
    },
    "throughput_MBps_month_over_125": {
        "gp2": 0.0,
        "gp3": 0.048,
        "io1": 0.0,
        "io2": 0.0,
        "st1": 0.0,
        "sc1": 0.0,
        "standard": 0.0,
    },
}
ebsIoKbSize = {
    "gp2": 256.0,
    "gp3": 256.0,
    "io1": 256.0,
    "io2": 256.0,
    "st1": 1024,
    "sc1": 1024,
    "standard": 1024,
}
# Can't differentiate sc1 and st1 as they return 'VolumeType': 'standard'. Picking the lower sc1 max throughput of 250 MBPs
ebsMaxMBPsThroughput = {
    "gp2": 250.0,
    "gp3": 1000.0,
    "io1": 1000.0,
    "io2": 1000.0,
    "st1": 500.0,
    "sc1": 250.0,
    "standard": 250.0,
}
nitroInstances = [
    "A1",
    "C5",
    "C5a",
    "C5ad",
    "C5d",
    "C5n",
    "C6g",
    "C6gd",
    "C6gn",
    "D3",
    "D3en",
    "G4",
    "I3en",
    "Inf1",
    "M5",
    "M5a",
    "M5ad",
    "M5d",
    "M5dn",
    "M5n",
    "M5zn",
    "M6g",
    "M6gd",
    "M6i",
    "p3dn.24xlarge",
    "P4",
    "R5",
    "R5a",
    "R5ad",
    "R5b",
    "R5d",
    "R5dn",
    "R5n",
    "R6g",
    "R6gd",
    "T3",
    "T3a",
    "T4g",
    "u",
    "X2gd",
    "z1d",
    "a1.metal",
    "c5.metal",
    "c5d.metal",
    "c5n.metal",
    "c6g.metal",
    "c6gd.metal",
    "i3.metal",
    "i3en.metal",
    "m5.metal",
    "m5d.metal",
    "m5dn.metal",
    "m5n.metal",
    "m5zn.metal",
    "m6g.metal",
    "m6gd.metal",
    "mac1.metal",
    "r5.metal",
    "r5b.metal",
    "r5d.metal",
    "r5dn.metal",
    "r5n.metal",
    "r6g.metal",
    "r6gd.metal",
    "u-6tb1.metal",
    "u-9tb1.metal",
    "u-12tb1.metal",
    "u-18tb1.metal",
    "u-24tb1.metal",
    "x2gd.metal",
    "z1d.metal",
]

regionName = "ap-southeast-2"

defaultRow = {
    #    "Account ID": "N/A",
    "Account Alias": "N/A",
    "Note": "N/A",
    "Note_2": "N/A",
    "EBS_data": "N/A",
    "Region": "N/A",
    "VolumeId": "N/A",
    "InstanceIds": "N/A",
    "InstanceTypes": "N/A",
    "HasNitro": False,
    "ProvisionedSize_GiB": -1,
    "ProvisionedType": "N/A",
    "MultiAttachEnabled": "N/A",
    "ProvisionedThroughput_MiB_perSec": -1.0,
    "VolumeReadBytesPeakOfSum_MiB_perSec": -1.0,
    "VolumeWriteBytesPeakOfSum_MiB_perSec": -1.0,
    "VolumeReadWriteBytesPeakOfSum_MiB_perSec": -1.0,
    "VolumeThroughputPercentage": -1.0,
    "ProvisionedIOPS": -1.0,
    "VolumeReadOpsPeakOfSum_perSec": -1.0,
    "VolumeWriteOpsPeakOfSum_perSec": -1.0,
    "VolumeReadWriteOpsPeakOfSum_perSec": -1.0,
    "MinBurstBalanceP99_percent": -1.0,
    "VolumeQueueLengthP99": -1.0,
    "VolumeQueueLengthAvg": -1.0,
    "VolumeReadLatencyAvg_ms": -1.0,
    # "VolumeReadLatencyNitroAvg_ms": -1.0,
    "VolumeWriteLatencyAvg_ms": -1.0,
    # "VolumeWriteLatencyNitroAvg_ms": -1.0,
    "cost_GB_month": -1.0,
    "cost_PIOPs_month": -1.0,
    "cost_Throughput_month": -1.0,
    "cost_est_monthly": -1.0,
    "gp3_price_equivalent": -1.0,
    "gp3_price_fit_to_demand": -1.0,
    "Tags": "N/A",
}


def compressStats(cwResponse, extendedStatistics, stats, getMax):
    # datapoints:
    # [{'Timestamp': datetime.datetime(2021, 8, 12, 18, 15, tzinfo=tzutc()), 'Average': 0.0001555555555555556, 'Unit': 'Count'}
    # {'Timestamp': datetime.datetime(2021, 8, 12, 23, 15, tzinfo=tzutc()), 'Average': 0.00043611111111111097, 'Unit': 'Count'}]
    if getMax:
        maxObserved = 0
    else:
        minObserved = 1000000000

    dataLength = len(cwResponse[0]["Datapoints"])
    for x in range(0, dataLength):
        r = cwResponse[0]["Datapoints"][x]

        if len(cwResponse) == 1:
            currentValue = (
                (r[stats[0]])
                if stats
                else (r["ExtendedStatistics"][extendedStatistics[0]])
            )
        elif len(cwResponse) == 2:
            r1 = cwResponse[1]["Datapoints"][x]
            # print(r)
            # print(r1)
            r_value = (
                (r[stats[0]])
                if stats
                else (r["ExtendedStatistics"][extendedStatistics[0]])
            )
            r1_value = (
                (r1[stats[0]])
                if stats
                else (r1["ExtendedStatistics"][extendedStatistics[0]])
            )
            currentValue = r_value + r1_value
            # print(currentValue)
        else:
            print("ERROR: cwResponse should be size 1 or 2")
            raise Exception("ERROR: cwResponse should be size 1 or 2")

        if getMax:
            if currentValue > maxObserved:
                maxObserved = currentValue
        else:
            if currentValue < minObserved:
                minObserved = currentValue
    if getMax:
        return maxObserved
    else:
        return minObserved


def get_read_lat(volumeId, client):
    # Avg Read Latency (s/Operation)
    # (Sum(VolumeTotalReadTime) / Sum(VolumeReadOps))
    response = client.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "e1",
                "Expression": "IF(m2==0, 0, m1 / m2)",
                "Label": "Avg Read Latency (ms/Operation)",
                "ReturnData": True,
                # 'Period': periodGlobal,
            },
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeTotalReadTime",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeTotalReadTime)",
                "ReturnData": False,
            },
            {
                "Id": "m2",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeReadOps",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeReadOps)",
                "ReturnData": False,
            },
        ],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        # NextToken='string',
    )
    return response


def get_write_lat(volumeId, client):
    # Avg Write Latency (s/Operation)
    # (Sum(VolumeTotalWriteTime) / Sum(VolumeWriteOps))
    response = client.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "e1",
                "Expression": "IF(m2==0, 0, m1 / m2)",
                "Label": "Avg Write Latency (ms/Operation)",
                "ReturnData": True,
                # 'Period': periodGlobal,
            },
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeTotalWriteTime",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeTotalWriteTime)",
                "ReturnData": False,
            },
            {
                "Id": "m2",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeWriteOps",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeWriteOps)",
                "ReturnData": False,
            },
        ],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        # NextToken='string',
    )
    return response


def get_sum_read_write_bytes(volumeId, client):
    response = client.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "e1",
                "Expression": "m1 + m2",
                "Label": "Sum read write bytes",
                "ReturnData": True,
            },
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeReadBytes",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeReadBytes)",
                "ReturnData": False,
            },
            {
                "Id": "m2",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeWriteBytes",
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": "Sum",
                },
                "Label": "sum(VolumeWriteBytes)",
                "ReturnData": False,
            },
        ],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        # NextToken='string',
    )
    return response


def get_cw_metric_data(volumeId, client, metricName, stat):
    # https://aws.amazon.com/premiumsupport/knowledge-center/cloudwatch-getmetricdata-api/
    response = client.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": metricName,
                        "Dimensions": [
                            {"Name": "VolumeId", "Value": volumeId},
                        ],
                    },
                    "Period": periodGlobal,
                    "Stat": stat,
                    # 'Unit': 'Seconds'|'Microseconds'|'Milliseconds'|'Bytes'|'Kilobytes'|'Megabytes'|'Gigabytes'|'Terabytes'|'Bits'|'Kilobits'|'Megabits'|'Gigabits'|'Terabits'|'Percent'|'Count'|'Bytes/Second'|'Kilobytes/Second'|'Megabytes/Second'|'Gigabytes/Second'|'Terabytes/Second'|'Bits/Second'|'Kilobits/Second'|'Megabits/Second'|'Gigabits/Second'|'Terabits/Second'|'Count/Second'|'None'
                },
                "Expression": "m2 / m1",
                "Label": "test",
                "ReturnData": True,
                # 'Period': periodGlobal,
            },
        ],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        # NextToken='string',
    )
    return response


def get_cw_metrics(volumeId, client, metricName, stats):
    response = client.get_metric_statistics(
        Namespace="AWS/EBS",
        MetricName=metricName,
        Dimensions=[{"Name": "VolumeId", "Value": volumeId}],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        Period=periodGlobal,  # 3600 seconds
        Statistics=stats,
    )
    return response


def get_cw_metrics_extended(volumeId, client, metricName, extendedStatistics):
    response = client.get_metric_statistics(
        Namespace="AWS/EBS",
        MetricName=metricName,
        Dimensions=[{"Name": "VolumeId", "Value": volumeId}],
        StartTime=datetime.datetime.now() - timedelta(days=7),
        EndTime=datetime.datetime.now(),
        Period=periodGlobal,  # 3600 seconds
        ExtendedStatistics=extendedStatistics,
    )
    return response


def get_gp3_price(throughput, iops, size):
    piops_0_to_3000 = min(iops, 3000)
    piops_3000_to_32000 = min(max(iops - 3000, 0), 29000)
    piops_32000_to_64000 = min(max(iops - 32000, 0), 32000)
    piops_over_64000 = max(iops - 64000, 0)

    cost_GB_month = pricingEbsSyd["size_gb_month"]["gp3"] * size
    cost_PIOPs_month = (
        pricingEbsSyd["piop_month_0_to_3000"]["gp3"] * piops_0_to_3000
        + pricingEbsSyd["piop_month_3000_to_32000"]["gp3"] * piops_3000_to_32000
        + pricingEbsSyd["piop_month_32000_to_64000"]["gp3"] * piops_32000_to_64000
        + pricingEbsSyd["piop_month_over_64000"]["gp3"] * piops_over_64000
    )
    cost_Throughput_month = pricingEbsSyd["throughput_MBps_month_over_125"][
        "gp3"
    ] * max(
        throughput - 125, 0
    )  # first 125MB/s-month are free
    cost_est_monthly = cost_GB_month + cost_PIOPs_month + cost_Throughput_month
    return cost_est_monthly


def get_all_volumes(ec2Client):
    volumes = []
    # Get all Volumes using paginator
    paginator = ec2Client.get_paginator("describe_volumes")
    page_iterator = paginator.paginate()
    for page in page_iterator:
        volumes.extend(page["Volumes"])
    return volumes


def get_all_instance_types(ec2Client):
    volumes = []
    # Get all Volumes using paginator
    paginator = ec2Client.get_paginator("describe_instance_types")
    page_iterator = paginator.paginate()
    for page in page_iterator:
        volumes.extend(page["Volumes"])
    return volumes


def get_ebs_data(profile):
    volumeIds = []

    # for region in regions:
    print(profile)
    session = boto3.Session(profile_name=profile, region_name="ap-southeast-2")
    ec2 = session.client("ec2", region_name="ap-southeast-2")
    cloudwatch = session.client("cloudwatch", region_name="ap-southeast-2")
    # result=ec2.describe_instances()
    # print(result)
    # Collect volume data
    response = get_all_volumes(ec2)
    print("Number of EBS found: {} ".format(len(response)))
    for r in response:
        # print(f'  {r}')
        # for v in r['Attachments']:
        try:
            volumeId = r["VolumeId"]
            print("Working on volumeId: {} ".format(volumeId))
            instanceIds = []
            instanceTypes = []
            for a in r["Attachments"]:
                instanceIds.append(a["InstanceId"])
                instanceType = ec2.describe_instance_attribute(
                    Attribute="instanceType", DryRun=False, InstanceId=a["InstanceId"]
                )["InstanceType"]["Value"]
                instanceTypes.append(instanceType)

            hasNitro = False
            for t in instanceTypes:
                if t.lower().startswith(tuple([x.lower() for x in nitroInstances])):
                    hasNitro = True
                    break

            vRb = get_cw_metrics(volumeId, cloudwatch, "VolumeReadBytes", ["Sum"])
            vWb = get_cw_metrics(volumeId, cloudwatch, "VolumeWriteBytes", ["Sum"])
            vRo = get_cw_metrics(volumeId, cloudwatch, "VolumeReadOps", ["Sum"])
            vWo = get_cw_metrics(volumeId, cloudwatch, "VolumeWriteOps", ["Sum"])
            vThr_pct = get_cw_metrics_extended(
                volumeId, cloudwatch, "VolumeThroughputPercentage", ["p99"]
            )

            vBb = get_cw_metrics_extended(volumeId, cloudwatch, "BurstBalance", ["p99"])
            vQl = get_cw_metrics_extended(
                volumeId, cloudwatch, "VolumeQueueLength", ["p99"]
            )
            vQl_avg = get_cw_metrics(
                volumeId, cloudwatch, "VolumeQueueLength", ["Average"]
            )

            # latency for Nitro instances calc is different to non-Nitro
            # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using_cloudwatch_ebs.html
            vRlat_avg = get_cw_metrics(
                volumeId, cloudwatch, "VolumeTotalReadTime", ["Average"]
            )
            vWlat_avg = get_cw_metrics(
                volumeId, cloudwatch, "VolumeTotalWriteTime", ["Average"]
            )

            note_2 = []
            try:
                vRlat_nitro_avg = get_read_lat(volumeId, cloudwatch)[
                    "MetricDataResults"
                ][0]["Values"]
                vWlat_nitro_avg = get_write_lat(volumeId, cloudwatch)[
                    "MetricDataResults"
                ][0]["Values"]
            except Exception as e:
                eMsg = "ERROR {} read/write latency nitro: {}".format(volumeId, repr(e))
                note_2.append(eMsg)
                print(eMsg)
                vRlat_nitro_avg = vRlat_avg
                vWlat_nitro_avg = vWlat_avg

            # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using_cloudwatch_ebs.html
            # Read Bandwidth (MiB/s) = Sum(VolumeReadBytes) / Period / 1024 / 1024
            # Compressing stats picks the max/min of data points found over time horizon
            # time horizon is 7 days baked into get_cw_metrics() and get_cw_metrics_extended()
            # where two arrays of data are provided, it will matrix-sum the data across two arrays before finding the max
            try:
                volumeReadBytesPeakOfSum_MiB_perSec = (
                    compressStats([vRb], [], ["Sum"], True) / periodGlobal / 1024 / 1024
                )
            except Exception as e:
                eMsg = "ERROR {} volumeReadBytesPeakOfSum_MiB_perSec: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                volumeReadBytesPeakOfSum_MiB_perSec = 0.0
            try:
                volumeWriteBytesPeakOfSum_MiB_perSec = (
                    compressStats([vWb], [], ["Sum"], True) / periodGlobal / 1024 / 1024
                )
            except Exception as e:
                eMsg = "ERROR {} volumeWriteBytesPeakOfSum_MiB_perSec: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                volumeWriteBytesPeakOfSum_MiB_perSec = 0.0
            try:
                vRWb = get_sum_read_write_bytes(volumeId, cloudwatch)[
                    "MetricDataResults"
                ][0]["Values"]
                volumeReadWriteBytesPeakOfSum_MiB_perSec = (
                    max(vRWb) / periodGlobal / 1024 / 1024
                )
            except Exception as e:
                eMsg = "ERROR {} volumeReadWriteBytesPeakOfSum_MiB_perSec: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                volumeReadWriteBytesPeakOfSum_MiB_perSec = 0.0

            # aggregation of Read/Write Ops is sum over the selected period, default period is 10 mins
            try:
                volumeReadOpsPeakOfSum_perSec = (
                    compressStats([vRo], [], ["Sum"], True) / periodGlobal
                )
            except Exception as e:
                eMsg = "ERROR {} volumeReadOpsPeakOfSum_perSec: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                volumeReadOpsPeakOfSum_perSec = 0.0
            try:
                volumeWriteOpsPeakOfSum_perSec = (
                    compressStats([vWo], [], ["Sum"], True) / periodGlobal
                )
            except Exception as e:
                eMsg = "ERROR {} volumeWriteOpsPeakOfSum_perSec: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                volumeWriteOpsPeakOfSum_perSec = 0.0

            iopsProvisioned = r["Iops"] if "Iops" in r else 0.0
            try:
                throughputMBPs_basedOnIOPs = min(
                    iopsProvisioned * ebsIoKbSize[r["VolumeType"]] / 1024,
                    ebsMaxMBPsThroughput[r["VolumeType"]],
                )
            except Exception as e:
                eMsg = "ERROR {} throughputMBPs_basedOnIOPs: {}".format(
                    volumeId, repr(e)
                )
                note_2.append(eMsg)
                print(eMsg)
                throughputMBPs_basedOnIOPs = 0.0

            # throughput separately defined for GP3 only
            throughput = (
                r["Throughput"] if "Throughput" in r else throughputMBPs_basedOnIOPs
            )

            # print(r['VolumeId'] + ' - ' + str(throughput) + ' - ' + str(throughputMBPs_basedOnIOPs))
            piops_0_to_3000 = min(iopsProvisioned, 3000)
            piops_3000_to_32000 = min(max(iopsProvisioned - 3000, 0), 29000)
            piops_32000_to_64000 = min(max(iopsProvisioned - 32000, 0), 32000)
            piops_over_64000 = max(iopsProvisioned - 64000, 0)
            volumeReadWriteOpsPeakOfSum_perSec = (
                compressStats([vRo, vWo], [], ["Sum"], True) / periodGlobal
            )

            cost_GB_month = pricingEbsSyd["size_gb_month"][r["VolumeType"]] * r["Size"]
            cost_PIOPs_month = (
                pricingEbsSyd["piop_month_0_to_3000"][r["VolumeType"]] * piops_0_to_3000
                + pricingEbsSyd["piop_month_3000_to_32000"][r["VolumeType"]]
                * piops_3000_to_32000
                + pricingEbsSyd["piop_month_32000_to_64000"][r["VolumeType"]]
                * piops_32000_to_64000
                + pricingEbsSyd["piop_month_over_64000"][r["VolumeType"]]
                * piops_over_64000
            )
            cost_Throughput_month = pricingEbsSyd["throughput_MBps_month_over_125"][
                r["VolumeType"]
            ] * max(
                throughput - 125, 0
            )  # first 125MB/s-month are free
            cost_est_monthly = cost_GB_month + cost_PIOPs_month + cost_Throughput_month

            volumeIds.append(
                {
                    # "Account ID": sub_account['Id'],
                    "Account Alias": profile,
                    "Note": "",
                    "Note_2": (" | ").join(note_2),
                    "EBS_data": r,
                    "Region": "ap-southeast-2",
                    "VolumeId": volumeId,
                    "InstanceIds": instanceIds,
                    "InstanceTypes": instanceTypes,
                    "HasNitro": hasNitro,
                    "ProvisionedSize_GiB": r["Size"],
                    "ProvisionedType": r["VolumeType"],
                    "MultiAttachEnabled": r["MultiAttachEnabled"],
                    "ProvisionedThroughput_MiB_perSec": throughput,
                    "VolumeReadBytesPeakOfSum_MiB_perSec": volumeReadBytesPeakOfSum_MiB_perSec,
                    "VolumeWriteBytesPeakOfSum_MiB_perSec": volumeWriteBytesPeakOfSum_MiB_perSec,
                    "VolumeReadWriteBytesPeakOfSum_MiB_perSec": volumeReadWriteBytesPeakOfSum_MiB_perSec,
                    "VolumeThroughputPercentage": compressStats(
                        [vThr_pct], ["p99"], [], True
                    ),
                    "ProvisionedIOPS": iopsProvisioned,
                    "VolumeReadOpsPeakOfSum_perSec": volumeReadOpsPeakOfSum_perSec,
                    "VolumeWriteOpsPeakOfSum_perSec": volumeWriteOpsPeakOfSum_perSec,
                    "VolumeReadWriteOpsPeakOfSum_perSec": volumeReadWriteOpsPeakOfSum_perSec,
                    "MinBurstBalanceP99_percent": compressStats(
                        [vBb], ["p99"], [], False
                    ),
                    "VolumeQueueLengthP99": compressStats([vQl], ["p99"], [], True),
                    "VolumeQueueLengthAvg": compressStats(
                        [vQl_avg], [], ["Average"], True
                    ),
                    "VolumeReadLatencyAvg_ms": max(vRlat_nitro_avg) * 1000
                    if hasNitro
                    else compressStats([vRlat_avg], [], ["Average"], True) * 1000,
                    # "VolumeReadLatencyNitroAvg_ms": max(vRlat_nitro_avg) * 1000,
                    "VolumeWriteLatencyAvg_ms": max(vWlat_nitro_avg) * 1000
                    if hasNitro
                    else compressStats([vWlat_avg], [], ["Average"], True) * 1000,
                    # "VolumeWriteLatencyNitroAvg_ms": max(vWlat_nitro_avg) * 1000,
                    "cost_GB_month": cost_GB_month,
                    "cost_PIOPs_month": cost_PIOPs_month,
                    "cost_Throughput_month": cost_Throughput_month,
                    "cost_est_monthly": cost_est_monthly,
                    "gp3_price_equivalent": get_gp3_price(
                        throughput, iopsProvisioned, r["Size"]
                    ),
                    "gp3_price_fit_to_demand": get_gp3_price(
                        volumeReadWriteBytesPeakOfSum_MiB_perSec,
                        volumeReadWriteOpsPeakOfSum_perSec,
                        r["Size"],
                    ),
                    "Tags": r["Tags"] if "Tags" in r else "No tags",
                }
            )
        except Exception as e:
            # print("error")
            # continue
            volumeId = r["VolumeId"] if "VolumeId" in r else "No volume id"
            errMsg = "ERROR: {}".format(repr(e))
            print(errMsg)
            appendRow = defaultRow.copy()
            # appendRow["Account ID"] = sub_account['Id']
            appendRow["Account Alias"] = profile
            appendRow["Note"] = errMsg
            appendRow["EBS_data"] = r
            appendRow["VolumeId"] = volumeId
            volumeIds.append(appendRow)

    return volumeIds


def get_ebs_data_all_accts():
    # get organization details
    results = []
    for i in boto3.session.Session().available_profiles:
        session1 = boto3.Session(profile_name=i, region_name="ap-southeast-2")
        appendRow = defaultRow.copy()
        appendRow["Account Alias"] = i
        results.append(appendRow)

        ebs_volume_data = get_ebs_data(profile=i)
        if ebs_volume_data is not None and ebs_volume_data:
            for row in ebs_volume_data:
                # appendRow = defaultRow.copy()
                results.append(row)
        else:
            appendRow = defaultRow.copy()
            # appendRow["Account ID"] = account['Id']
            appendRow["Account Alias"] = i
            appendRow["Note"] = "No ebs volume data"
            results.append(appendRow)

        print(" ")

    # generate CSV file
    csv_file = "EBS-Data" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
    csv_columns = defaultRow.keys()
    try:
        with open(csv_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in results:
                if data is not None:
                    # print(data)
                    writer.writerow(data)

        print("")
        print("Complete!")
        print("Result file: {}".format(csv_file))
    except IOError:
        print("ERROR Failed to export CSV file due to I/O error")
        exit(2)


if __name__ == "__main__":
    print("INFO Starting")

    get_ebs_data_all_accts()
