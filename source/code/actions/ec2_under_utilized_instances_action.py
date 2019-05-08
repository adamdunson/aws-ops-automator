######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

import StringIO
import csv
from datetime import timedelta

import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs.report_output_writer import report_key_name

INFO_ACTION_START = "Checking utilisation of EC2 Instances for account {} in region {} for task {}"
INF_LOW_CPU_IO = "Instance {} has {} low CPU Utilization and {} low Network IO days"
INF_LOW_IO = "Instance {} has {} low Network IO days"
INF_LOW_CPU = "Instance {} has {} low CPU Utilization days"
INF_TAGGING_INSTANCES = "Tagging instances {} with tags {}"

MEGABYTE = 1024 * 1024
ONE_DAY = 24 * 3600
MAX_METRICS_PERIOD = 455

PARAM_DESC_CPU_PERC = "Minimum CPU Utilization threshold percentage per day in test period."
PARAM_DEC_STRATEGY = "Strategy for checking CPU Utilization and/or Network IO for underutilized instances. If set to CPU_ONLY, " \
                     "or IO_ONLY the instance is considered underutilized if the CPU utilization or network IO is below their " \
                     "specified parameter value. If set to CPU_AND_IO the instances is considered as underutilized if both the " \
                     "CPU utilization AND network IO are both below their specified parameter values. I set to CPU_OR_IO " \
                     "then the instance is considered as underutilized if any of the CPU utilization OR the network IO are below " \
                     "he specified parameter values."
PARAM_DESC_INSTANCE_TAGS = "Tags to create for underutilized EC2 instances."
PARAM_DESC_NETWORK_IO = "Minimum Network IO (MB) per day in test period."
PARAM_DESC_THRESHOLD_DAYS = "Maximum number of days in test period on which the instances may have low CPU utilization or " \
                            "Network IO (1-455)."
PARAM_DESC_TEST_PERIOD_DAYS = "Test period in days in which there may be the maximum of days with low utilization. (1-455)"
PARAM_DESC_REPORT = "Create CSV output report including underutilized instance data"
PARAM_DESC_REPORT_TAGS = "Comma separated list of instance tags names to include in output report"

PARAM_LABEL_CPU_PERC = "Minimum CPU Utilization"
PARAM_LABEL_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_NETWORK_IO = "Minimum Network IO (MB)"
PARAM_LABEL_THRESHOLD_DAYS = "Max low utilization days"
PARAM_LABEL_STRATEGY = "Utilization checks"
PARAM_LABEL_TEST_PERIOD_DAYS = "Utilization test period"
PARAM_LABEL_REPORT = "Create CSV report"
PARAM_LABEL_REPORT_TAGS = "Reported Tags"

PARAM_CPU_PERC = "CpuLowUtilizationPerc"
PARAM_NETWORK_IO = "NetworkIOLowUtilizationMB"
PARAM_STRATEGY = "Strategy"
PARAM_THRESHOLD_DAYS = "ThresholdDays"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_TEST_PERIOD_DAYS = "TestPeriodDays"
PARAM_REPORT = "WriteOutputReport"
PARAM_REPORT_TAGS = "ReportedTags"

GROUP_TITLE_UTILISATION_OPTIONS = "Utilization check settings"
GROUP_TITLE_REPORT_OPTIONS = "Output report"

CPU_ONLY = "CPU_ONLY"
IO_ONLY = "IO_ONLY"
CPU_AND_IO = "CPU_AND_IO"
CPU_OR_IO = "CPU_OR_IO"

DEFAULT_TEST_PERIOD = 14
DEFAULT_THRESHOLD_DAYS = 4
DEFAULT_LOW_CPU = 10
DEFAULT_LOW_IO = 5
DEFAULT_STRATEGY = CPU_AND_IO


class Ec2UnderUtilizedInstancesAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Underutilized Instances",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Tags and reports EC2 instances that do have low CPU Utilization and/or Network IO",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "9e36a722-e622-483f-81eb-1d80c74c2935",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "Reservations[*].Instances[].{InstanceId:InstanceId, Tags:Tags,"
                                  "State:State.Name}|[?State!='terminated']",

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_SELECT_SIZE: [ACTION_SIZE_STANDARD,
                             ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE
                             ] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_STANDARD],

        ACTION_PARAMETERS: {

            PARAM_TEST_PERIOD_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_TEST_PERIOD_DAYS,
                PARAM_TYPE: int,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: str(DEFAULT_TEST_PERIOD),
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: MAX_METRICS_PERIOD,
                PARAM_LABEL: PARAM_LABEL_TEST_PERIOD_DAYS
            },
            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: "",
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
            },

            PARAM_THRESHOLD_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_THRESHOLD_DAYS,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: MAX_METRICS_PERIOD,
                PARAM_DEFAULT: str(DEFAULT_THRESHOLD_DAYS),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_THRESHOLD_DAYS
            },
            PARAM_CPU_PERC: {
                PARAM_DESCRIPTION: PARAM_DESC_CPU_PERC,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: 99,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: str(DEFAULT_LOW_CPU),
                PARAM_LABEL: PARAM_LABEL_CPU_PERC
            },
            PARAM_NETWORK_IO: {
                PARAM_DESCRIPTION: PARAM_DESC_NETWORK_IO,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: str(DEFAULT_LOW_IO),
                PARAM_LABEL: PARAM_LABEL_NETWORK_IO
            },
            PARAM_STRATEGY: {
                PARAM_DESCRIPTION: PARAM_DEC_STRATEGY,
                PARAM_TYPE: type(""),
                PARAM_ALLOWED_VALUES: [CPU_AND_IO, CPU_OR_IO, CPU_ONLY, IO_ONLY],
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: DEFAULT_STRATEGY,
                PARAM_LABEL: PARAM_LABEL_STRATEGY
            },
            PARAM_REPORT: {
                PARAM_DESCRIPTION: PARAM_DESC_REPORT,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT
            },
            PARAM_REPORT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_REPORT_TAGS,
                PARAM_TYPE: list,
                PARAM_DEFAULT: "",
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_UTILISATION_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_TEST_PERIOD_DAYS,
                    PARAM_THRESHOLD_DAYS,
                    PARAM_CPU_PERC,
                    PARAM_NETWORK_IO,
                    PARAM_STRATEGY,
                    PARAM_INSTANCE_TAGS
                ]
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_REPORT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_REPORT,
                    PARAM_REPORT_TAGS
                ]
            }

        ],

        ACTION_PERMISSIONS: ["cloudwatch:GetMetricData",
                             "ec2:CreateTags",
                             "ec2:DeleteTags"],

    }

    @staticmethod
    def action_validate_parameters(parameters, _, __):
        test_period = parameters[PARAM_TEST_PERIOD_DAYS]
        threshold_days = parameters[PARAM_THRESHOLD_DAYS]

        if threshold_days > test_period:
            raise ValueError("Number of threshold days must be less or equal to days in test period")
        return parameters

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instances = self._resources_
        self.instance_ids = [v["InstanceId"] for v in self.instances]

        self._ec2_client = None
        self._metrics_client = None

        self.check_period_days = int(self.get(PARAM_TEST_PERIOD_DAYS, DEFAULT_TEST_PERIOD))
        self.threshold_days = int(self.get(PARAM_THRESHOLD_DAYS, DEFAULT_THRESHOLD_DAYS))
        self.low_cpu = int(self.get(PARAM_CPU_PERC, DEFAULT_LOW_CPU))
        self.low_io = int(self.get(PARAM_NETWORK_IO, DEFAULT_LOW_IO)) * MEGABYTE
        self.strategy = self.get(PARAM_STRATEGY, DEFAULT_STRATEGY)
        self.report = self.get(PARAM_REPORT, False)
        self.reported_tags = [t.strip() for t in self.get(PARAM_REPORT_TAGS, [])]

        self.under_utilized_instances = []
        self.low_cpu_instance_list = {}
        self.low_io_instance_list = {}

        self._report_writer = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "instances-checked": len(self.instance_ids),
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = ["create_tags",
                       "delete_tags"]

            self._ec2_client = get_client_with_retries("ec2", methods, region=self._region_,
                                                       session=self._session_, logger=self._logger_)
        return self._ec2_client

    @property
    def report_writer(self):
        if self._report_writer is None:
            self._report_writer = get_report_output_writer(context=self._context_, logger=self._logger_)
        return self._report_writer

    @property
    def metrics_client(self):
        if self._metrics_client is None:
            methods = ["get_metric_data"]
            self._metrics_client = get_client_with_retries("cloudwatch", methods, region=self._region_,
                                                           session=self._session_, logger=self._logger_)
        return self._metrics_client

    def get_meta_data_queries(self):
        query_data = []

        for instance_id in self.instance_ids:
            i = instance_id.replace("-", "")
            if self.strategy != CPU_ONLY:
                query_data += [
                    {
                        "Id": "io{}".format(i),
                        "Expression": "in{}+out{}".format(i, i)
                    },
                    {

                        "Id": "in{}".format(i),
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/EC2",
                                "MetricName": "NetworkIn",
                                "Dimensions": [
                                    {
                                        "Name": "InstanceId",
                                        "Value": instance_id
                                    }
                                ]
                            },
                            "Period": ONE_DAY,
                            "Stat": "Sum"
                        },
                        "ReturnData": False
                    },
                    {

                        "Id": "out{}".format(i),
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/EC2",
                                "MetricName": "NetworkOut",
                                "Dimensions": [
                                    {
                                        "Name": "InstanceId",
                                        "Value": instance_id
                                    }
                                ]
                            },
                            "Period": ONE_DAY,
                            "Stat": "Sum"
                        },
                        "ReturnData": False
                    }]

            if self.strategy != IO_ONLY:
                query_data += [
                    {

                        "Id": "cpu{}".format(i),
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/EC2",
                                "MetricName": "CPUUtilization",
                                "Dimensions": [
                                    {
                                        "Name": "InstanceId",
                                        "Value": instance_id
                                    }
                                ]
                            },
                            "Period": ONE_DAY,
                            "Stat": "Average"
                        },
                        "ReturnData": True
                    }]

            if len(query_data) < 96:
                continue

            yield query_data
            query_data = []

        if len(query_data) > 0:
            yield query_data

    def _act_on_underutilized_instances(self):
        if len(self.under_utilized_instances) > 0:
            tags = self.build_tags_from_template(parameter_name=PARAM_INSTANCE_TAGS, tag_variables={})

            self._logger_.info(INF_TAGGING_INSTANCES, ",".join(self.under_utilized_instances), safe_json(tags, 3))

            tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                 resource_ids=self.under_utilized_instances,
                                 tags=tags,
                                 logger=self._logger_)

    def _process_instance_metrics_data(self):

        def add_result(instance_id, low_cpu_days=None, low_io_days=None):
            if "instances" not in self.result:
                self.result["instances"] = {}
            if instance_id not in self.result["instances"]:
                self.result["instances"][instance_id] = {}
            if low_cpu_days is not None:
                self.result["instances"][instance_id]["cpu"] = low_cpu_days
            if low_io_days is not None:
                self.result["instances"][instance_id]["io"] = low_io_days

        if self.strategy == CPU_ONLY:
            self.under_utilized_instances = self.low_cpu_instance_list.keys()
            for i in self.under_utilized_instances:
                self._logger_.info(INF_LOW_CPU, i, self.low_cpu_instance_list[i])
                add_result(i, low_cpu_days=self.low_cpu_instance_list[i])

        elif self.strategy == IO_ONLY:
            self.under_utilized_instances = self.low_io_instance_list.keys()
            for i in self.under_utilized_instances:
                self._logger_.info(INF_LOW_IO, i, self.low_io_instance_list[i])
                add_result(i, low_io_days=self.low_io_instance_list[i])

        else:
            if self.strategy == CPU_AND_IO:
                self.under_utilized_instances = [k for k in self.low_cpu_instance_list.keys() if
                                                 k in self.low_io_instance_list.keys()]
            elif self.strategy == CPU_OR_IO:
                self.under_utilized_instances = self.low_cpu_instance_list.keys()
                for i in self.low_io_instance_list.keys():
                    if i not in self.under_utilized_instances:
                        self.under_utilized_instances.append(i)

            for i in self.under_utilized_instances:
                self._logger_.info(INF_LOW_CPU_IO, i, self.low_cpu_instance_list.get(i, 0), self.low_io_instance_list.get(i, 0))
                add_result(i, low_cpu_days=self.low_cpu_instance_list.get(i, 0), low_io_days=self.low_io_instance_list.get(i, 0))

    def _collect_instances_metric_data(self):
        for queries in self.get_meta_data_queries():
            metrics_data = self.metrics_client.get_metric_data_with_retries(
                MetricDataQueries=queries,
                StartTime=self._datetime_.now() - timedelta(days=self.check_period_days),
                EndTime=self._datetime_.now()).get("MetricDataResults", [])

            for metrics_item in metrics_data:

                # must have enough data for running days in threshold
                if len(metrics_item["Values"]) >= self.threshold_days:

                    # cpu utilization
                    if metrics_item["Id"].startswith("cpu"):
                        low_cpu_days = len(
                            [cpu_util_perc for cpu_util_perc in metrics_item["Values"] if int(cpu_util_perc) < self.low_cpu])
                        if low_cpu_days >= self.threshold_days:
                            inst_id = "i-{}".format(metrics_item["Id"][4:])
                            self.low_cpu_instance_list[inst_id] = low_cpu_days

                    # network io
                    elif metrics_item["Id"].startswith("io"):
                        low_io_days = len([network_io for network_io in metrics_item["Values"] if int(network_io) < self.low_io])
                        if low_io_days >= self.threshold_days:
                            inst_id = "i-{}".format(metrics_item["Id"][3:])
                            self.low_io_instance_list[inst_id] = low_io_days

    def _create_instance_report(self):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)

        # build header row
        instance_data_headers = ["AwsAccount",
                                 "Region",
                                 "InstanceId",
                                 "InstanceType"]

        low_utilization_data_headers = ["Period",
                                        "ThresholdDays",
                                        "LowCpuPercentage",
                                        "LowNetworkIOMB",
                                        "LowCpuDays",
                                        "LowNetworkIODays"]
        csv_data.writerow(["DateTime"] + instance_data_headers + low_utilization_data_headers + self.reported_tags)

        if len(self.under_utilized_instances) > 0:

            # get data for underutilized instances
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

            instances = ec2.describe(services.ec2_service.INSTANCES,
                                     InstanceIds=self.under_utilized_instances,
                                     region=self._region_)

            # date and time
            dt = self._datetime_.now().replace(second=0, microsecond=0)
            date_time = [dt.isoformat()]

            for i in instances:
                instance_id = i["InstanceId"]
                # instance and tag data
                instance_data, tag_data = get_resource_data(i, instance_data_headers, self.reported_tags)

                # low utilization data
                low_utilization_data = [self.check_period_days,
                                        self.threshold_days,
                                        self.low_cpu,
                                        int(self.low_io / MEGABYTE),
                                        self.low_cpu_instance_list.get(instance_id, ""),
                                        self.low_io_instance_list.get(instance_id, "")]

                # add the row
                row = date_time + instance_data + low_utilization_data + tag_data
                csv_data.writerow(row)

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INFO_ACTION_START, self._account_,
                           self._region_, self._task_)

        self._collect_instances_metric_data()

        self._process_instance_metrics_data()

        self._act_on_underutilized_instances()

        if self.report:
            self._create_instance_report()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CheckedInstances=len(self.instances),
            UnderUtilizedInstances=len(self.under_utilized_instances))

        return self.result
