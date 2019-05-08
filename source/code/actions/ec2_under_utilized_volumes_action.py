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

INF_TAGGING_VOLUMES = "Tagging volumes {} with tags {}"
INF_ACTION_START = "Checking utilisation of EBS volumes for account {} in region {} for task {}"

GROUP_TITLE_UTILISATION_OPTIONS = "Utilization check settings"
GROUP_TITLE_REPORT_OPTIONS = "Output report"

PARAM_DESC_IDLE_DAYS = "Number of days in which volumes had no read or write IOPS (1-455)."
PARAM_DESC_VOLUME_TAGS = "Tags to create on underutilized EBS Volume."
PARAM_DESC_REPORT = "Create CSV output report including underutilized volume data"
PARAM_DESC_REPORT_TAGS = "Comma separated list of volume tags names to include in output report"

PARAM_LABEL_IDLE_DAYS = "Days"
PARAM_LABEL_VOLUME_TAGS = "Volume tags"
PARAM_LABEL_REPORT = "Create CSV report"
PARAM_LABEL_REPORT_TAGS = "Reported Tags"

PARAM_IDLE_DAYS = "NoIopsDays"
PARAM_VOLUME_TAGS = "VolumeTags"
PARAM_REPORT = "WriteOutputReport"
PARAM_REPORT_TAGS = "ReportedTags"


class Ec2UnderUtilizedVolumesAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Underutilized Volumes",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Tags volumes that had no read or write IOPS",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "0e92d2c7-3499-4217-b110-1d85495ad16c",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.VOLUMES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "Volumes[*].{VolumeId:VolumeId,Tags:Tags}",

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_STANDARD,
                             ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_STANDARD],

        ACTION_PARAMETERS: {

            PARAM_VOLUME_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_VOLUME_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: True,
                PARAM_LABEL: PARAM_LABEL_VOLUME_TAGS
            },
            PARAM_IDLE_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_IDLE_DAYS,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: 455,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_IDLE_DAYS
            },
            PARAM_REPORT: {
                PARAM_DESCRIPTION: PARAM_DESC_REPORT,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: "False",
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT
            },
            PARAM_REPORT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_REPORT_TAGS,
                PARAM_TYPE: list,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_UTILISATION_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_IDLE_DAYS,
                    PARAM_VOLUME_TAGS
                ],

            },

            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_REPORT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_REPORT,
                    PARAM_REPORT_TAGS
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "cloudwatch:GetMetricData",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.volumes = self._resources_
        self.volume_ids = [v["VolumeId"] for v in self.volumes]

        self._ec2_client = None
        self._metrics_client = None

        self.no_iops_period_days = int(self.get(PARAM_IDLE_DAYS))

        self.report = self.get(PARAM_REPORT, False)
        self.reported_tags = [t.strip() for t in self.get(PARAM_REPORT_TAGS, [])]

        self._report_writer = None

        self.under_utilized_volumes = []

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "volumes-checked": len(self.volume_ids)
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "create_tags",
                "delete_tags"
            ]
            self._ec2_client = get_client_with_retries("ec2", methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @property
    def report_writer(self):
        if self._report_writer is None:
            self._report_writer = get_report_output_writer(context=self._context_, logger=self._logger_)
        return self._report_writer

    @property
    def metrics_client(self):
        if self._metrics_client is None:
            methods = [
                "get_metric_data"
            ]
            self._metrics_client = get_client_with_retries("cloudwatch",
                                                           methods,
                                                           region=self._region_,
                                                           session=self._session_,
                                                           logger=self._logger_)
        return self._metrics_client

    def get_meta_data_queries(self):
        query_data = []

        for vol in self.volume_ids:
            v = vol.replace("-", "")
            query_data += [
                {
                    "Id": v,
                    "Expression": "r{}+w{}".format(v, v)
                },
                {

                    "Id": "r{}".format(v),
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeReadOps",
                            "Dimensions": [
                                {
                                    "Name": "VolumeId",
                                    "Value": vol
                                }
                            ]
                        },
                        "Period": 24 * 3600,
                        "Stat": "Sum"
                    },
                    "ReturnData": False
                },
                {

                    "Id": "w{}".format(v),
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeWriteOps",
                            "Dimensions": [
                                {
                                    "Name": "VolumeId",
                                    "Value": vol
                                }
                            ]
                        },
                        "Period": 24 * 3600,
                        "Stat": "Sum"
                    },
                    "ReturnData": False
                }

            ]

            if len(query_data) < 97:
                continue

            yield query_data
            query_data = []

        if len(query_data) > 0:
            yield query_data

    def _create_volume_report(self):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)

        # build header row
        volume_data_headers = ["AwsAccount", "Region", "VolumeId", "VolumeType", "Iops", "Size"]
        low_utilization_data_headers = ["NoIOPSDays"]
        csv_data.writerow(["DateTime"] + volume_data_headers + low_utilization_data_headers + self.reported_tags)

        if len(self.under_utilized_volumes) > 0:
            # get data for underutilized instances
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

            volumes = ec2.describe(services.ec2_service.VOLUMES,
                                   VolumeIds=self.under_utilized_volumes,
                                   region=self._region_)

            # date and time
            dt = self._datetime_.now().replace(second=0, microsecond=0)
            date_time = [dt.isoformat()]

            for v in volumes:
                # instance and tag data
                volume_data, tag_data = get_resource_data(v, volume_data_headers, self.reported_tags)

                # low utilization data
                low_utilization_data = [self.no_iops_period_days]

                # add the row
                row = date_time + volume_data + low_utilization_data + tag_data
                csv_data.writerow(row)

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_ACTION_START, self._account_, self._region_, self._task_)

        for queries in self.get_meta_data_queries():
            start_time = date_time_provider().now() - timedelta(days=self.no_iops_period_days)
            end_time = self._datetime_.now()
            metrics_data = self.metrics_client.get_metric_data_with_retries(MetricDataQueries=queries,
                                                                            StartTime=start_time,
                                                                            EndTime=end_time).get("MetricDataResults",[])

            self.under_utilized_volumes += ["vol-{}".format(m["Id"][3:]) for m in metrics_data if
                                            sum(m.get("Values", [])) < 1]

        if len(self.under_utilized_volumes) > 0:
            tags = self.build_tags_from_template(parameter_name=PARAM_VOLUME_TAGS, tag_variables={})

            self._logger_.info(INF_TAGGING_VOLUMES, ",".join(self.under_utilized_volumes), safe_json(tags, 3))

            tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                 resource_ids=self.under_utilized_volumes,
                                 tags=tags,
                                 logger=self._logger_)

            if self.report:
                self._create_volume_report()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CheckedVolumes=len(self.volumes),
            UnderUtilizedVolumes=len(self.under_utilized_volumes)
        )

        self.result["underutilized-volumes"] = self.under_utilized_volumes

        return self.result
