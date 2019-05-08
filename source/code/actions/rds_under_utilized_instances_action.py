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

import dateutil.parser

import pytz
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from helpers import safe_json
from outputs.report_output_writer import report_key_name

INF_TAGGING_INSTANCES = "Tagging RDS instances {} with tags {}"
INF_ACTION_START = "Checking utilisation of RDS instances for account {} in region {} for task {}"

GROUP_TITLE_UTILISATION_OPTIONS = "Utilization check settings"
GROUP_TITLE_REPORT_OPTIONS = "Output report"

PARAM_DESC_IDLE_DAYS = "Number of days in which instances had no connections (1-455)."
PARAM_DESC_INSTANCE_TAGS = "Tags to create on underutilized RDS instances."
PARAM_DESC_REPORT = "Create CSV output report including underutilized instance data."
PARAM_DESC_REPORT_TAGS = "Instance tags to include in output report."
PARAM_DESC_INCLUDE_STOPPED = "Include RDS instances that are currently in stopped state."

PARAM_LABEL_IDLE_DAYS = "Days"
PARAM_LABEL_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_REPORT = "Create CSV report"
PARAM_LABEL_REPORT_TAGS = "Reported Tags"
PARAM_LABEL_INCLUDE_STOPPED = "Include stopped instances"

PARAM_IDLE_DAYS = "NoConnectionDays"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_REPORT = "WriteOutputReport"
PARAM_REPORT_TAGS = "ReportedTags"
PARAM_INCLUDE_STOPPED = "IncludeStopped"


class RdsUnderUtilizedInstancesAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Underutilized Instances",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Tags and reports RDS instances that had no connections",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "c18570cf-24c6-458a-b382-84ea8ba948fb",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "DBInstances[*].{DBInstanceIdentifier:DBInstanceIdentifier,"
                                  "DBInstanceArn:DBInstanceArn,"
                                  "DbiResourceId:DbiResourceId,"
                                  "DBName:DBName,"
                                  "DBInstanceStatus:DBInstanceStatus,"
                                  "Engine:Engine,"
                                  "LicenseModel:LicenseModel,"
                                  "InstanceCreateTime:InstanceCreateTime,"
                                  "DBInstanceClass:DBInstanceClass,"
                                  "StorageType:StorageType,"
                                  "Endpoint:Endpoint.Address,"
                                  "AllocatedStorage:AllocatedStorage}",

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_STANDARD,
                             ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_STANDARD],

        ACTION_PARAMETERS: {

            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: True,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
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
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT
            },
            PARAM_INCLUDE_STOPPED: {
                PARAM_DESCRIPTION: PARAM_DESC_INCLUDE_STOPPED,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: "False",
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INCLUDE_STOPPED
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
                    PARAM_INSTANCE_TAGS
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
            "tag:GetResources",
            "rds:AddTagsToResource",
            "rds:RemoveTagsFromResource"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_instances = self._resources_
        self.db_instance_ids = [db["DBInstanceIdentifier"] for db in self.db_instances]

        self._rds_client = None
        self._metrics_client = None

        # tags to set to  instance when idle
        self.no_connection_days = int(self.get(PARAM_IDLE_DAYS))

        self.report = self.get(PARAM_REPORT, False)
        self.reported_tags = [t.strip() for t in self.get(PARAM_REPORT_TAGS, [])]

        self.include_stopped = self.get(PARAM_INCLUDE_STOPPED, False)

        self._report_writer = None

        self.under_utilized_db_instances = []

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "rds-instances-checked": len(self.db_instance_ids)
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def rds_client(self):
        if self._rds_client is None:
            self._rds_client = get_client_with_retries("rds",
                                                       methods=["add_tags_to_resource",
                                                                "remove_tags_from_resource"],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._rds_client

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

        for db_instance in [db for db in self.db_instances if db["DBInstanceStatus"] != "stopped" or self.include_stopped]:
            query_data += [

                {
                    "Id": db_instance["DbiResourceId"].replace("-", ""),
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/RDS",
                            "MetricName": "DatabaseConnections",
                            "Dimensions": [
                                {
                                    "Name": "DBInstanceIdentifier",
                                    "Value": db_instance["DBInstanceIdentifier"]
                                }
                            ]
                        },
                        "Period": 24 * 3600,
                        "Stat": "Sum",
                        "Unit": "Count"
                    },
                    "ReturnData": True
                }
            ]

            if len(query_data) < 99:
                continue

            yield query_data
            query_data = []

        if len(query_data) > 0:
            yield query_data

    def _create_instance_report(self):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)

        # build header row
        rds_instance_data_headers = [
            "AwsAccount",
            "Region",
            "DBInstanceIdentifier",
            "DbiResourceId",
            "DBInstanceArn",
            "DBName",
            "DBInstanceClass",
            "Engine",
            "LicenseModel",
            "InstanceCreateTime",
            "AllocatedStorage",
            "StorageType",
            "Endpoint",
            "DBInstanceStatus"
        ]

        low_utilization_data_headers = ["NoConnectionDays"]
        csv_data.writerow(["DateTime"] + rds_instance_data_headers + low_utilization_data_headers + self.reported_tags)

        dt = self._datetime_.now().replace(second=0, microsecond=0)
        date_time = [dt.isoformat()]

        for db in self.under_utilized_db_instances:
            # instance and tag data
            instance_data, tag_data = get_resource_data(db, rds_instance_data_headers, self.reported_tags)

            # low utilization data
            low_utilization_data = [self.no_connection_days]

            # add the row
            row = date_time + instance_data + low_utilization_data + tag_data
            csv_data.writerow(row)

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_ACTION_START, self._account_, self._region_, self._task_)

        offset_date = self._datetime_.utcnow().replace(tzinfo=pytz.UTC) - timedelta(days=self.no_connection_days - 1)

        start_time = self._datetime_.now() - timedelta(days=self.no_connection_days)
        end_time = self._datetime_.now()

        for queries in self.get_meta_data_queries():

            metrics_data = self.metrics_client.get_metric_data_with_retries(MetricDataQueries=queries,
                                                                            StartTime=start_time,
                                                                            EndTime=end_time).get("MetricDataResults", [])

            for metrics_item in metrics_data:
                connections = metrics_item.get("Values", [])
                if len(connections) > 0 and sum(connections) == 0:
                    resource_id = "db-{}".format(metrics_item["Id"][2:])
                    for d in self.db_instances:
                        if d["DbiResourceId"] == resource_id:
                            if dateutil.parser.parse(str(d["InstanceCreateTime"])).replace(tzinfo=pytz.UTC) < offset_date:
                                self.under_utilized_db_instances.append(d)
                            break

        if len(self.under_utilized_db_instances) > 0:
            tags = self.build_tags_from_template(parameter_name=PARAM_INSTANCE_TAGS,
                                                 tag_variables={})

            self._logger_.info(INF_TAGGING_INSTANCES,
                               ",".join([i["DBInstanceIdentifier"] for i in self.under_utilized_db_instances]),
                               safe_json(tags, 3))

            tagging.set_rds_tags(rds_client=self.rds_client,
                                 resource_arns=[i["DBInstanceArn"] for i in self.under_utilized_db_instances],
                                 tags=tags,
                                 logger=self._logger_)

            if self.report:
                self._create_instance_report()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CheckedInstances=len(self.db_instances),
            UnderUtilizedRdsInstances=len(self.under_utilized_db_instances)
        )

        self.result["underutilized-rds-instances"] = self.under_utilized_db_instances

        return self.result
