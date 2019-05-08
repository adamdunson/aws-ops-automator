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

import services.ec2_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs.report_output_writer import report_key_name

INF_ACTION_START = "Checking unassigned elastic IP addresses for account {} in region {} for task {}"

GROUP_TITLE_UTILISATION_OPTIONS = "IP association check settings"
GROUP_TITLE_REPORT_OPTIONS = "Output report"

PARAM_DESC_RELEASE_ADDRESS = "Release unassigned elastic IP addresses"
PARAM_DESC_REPORT = "Create CSV output report including unassigned IP address data"
PARAM_DESC_REPORT_TAGS = "Comma separated list of volume tags names to include in output report"

PARAM_LABEL_RELEASE_ADDRESS = "Released unassigned addresses"
PARAM_LABEL_REPORT = "Create CSV report"
PARAM_LABEL_REPORT_TAGS = "Reported Tags"

PARAM_RELEASE_ADDRESS = "ReleaseUnassigned"
PARAM_REPORT = "WriteOutputReport"
PARAM_REPORT_TAGS = "ReportedTags"


class Ec2DisassociatedIpsAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Disassociated Elastic IP addresses",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Reports and releases IP addresses that are not assigned to EC2 instances",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "9f27f6cf-68fc-4dbd-8f57-e6b41a40b6f7",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.ADDRESSES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "Addresses[*].{AllocationId:AllocationId, "
                                  "AssociationId:AssociationId, "
                                  "PublicIp:PublicIp, "
                                  "InstanceId:InstanceId,"
                                  "Tags:Tags}",

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_PARAMETERS: {

            PARAM_RELEASE_ADDRESS: {
                PARAM_DESCRIPTION: PARAM_DESC_RELEASE_ADDRESS,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: "False",
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_RELEASE_ADDRESS
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
                    PARAM_RELEASE_ADDRESS
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

        ACTION_PERMISSIONS: ["ec2:DescribeAddresses",
                             "ec2:ReleaseAddress"],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.addresses = self._resources_
        self.allocation_ids = [v["AllocationId"] for v in self.addresses]

        self._ec2_client = None

        self.release_addresses = int(self.get(PARAM_RELEASE_ADDRESS))

        self.report = self.get(PARAM_REPORT, False)
        self.reported_tags = [t.strip() for t in self.get(PARAM_REPORT_TAGS, [])]

        self.disassociated_adresses = []

        self._report_writer = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "checked-addresses": len(self.allocation_ids)
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=[
                                                           "release_address"
                                                       ],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @property
    def report_writer(self):
        if self._report_writer is None:
            self._report_writer = get_report_output_writer(context=self._context_, logger=self._logger_)
        return self._report_writer

    def _create_volume_report(self):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)

        # build header row
        address_data_headers = ["AwsAccount", "Region", "AllocationId", "PublicIp"]
        csv_data.writerow(["DateTime"] + address_data_headers + self.reported_tags)

        if len(self.disassociated_adresses) > 0:

            # date and time
            dt = self._datetime_.now().replace(second=0, microsecond=0)
            date_time = [dt.isoformat()]

            for a in self.disassociated_adresses:
                # instance and tag data
                address_data, tag_data = get_resource_data(a, address_data_headers, self.reported_tags)

                # add the row
                row = date_time + address_data + tag_data
                csv_data.writerow(row)

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def release_disassociated_addresses(self):
        for a in self.disassociated_adresses:
            self.ec2_client.release_address_with_retries(
                AllocationId=a["AllocationId"])

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_ACTION_START, self._account_, self._region_, self._task_)

        self.disassociated_adresses = [a for a in self.addresses if a.get("AssociationId", None) is None]

        if len(self.disassociated_adresses) > 0:

            if self.release_addresses:
                self.release_disassociated_addresses()

            if self.report:
                self._create_volume_report()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CheckedAddresses=len(self.addresses),
            DissassiciatedAddresses=len(self.disassociated_adresses)
        )

        self.result["disassociated-addresses"] = [
            {
                "PublicIp": a["PublicIp"],
                "AllocationId": a["AllocationId"]
            } for a in self.disassociated_adresses
        ]

        return self.result
