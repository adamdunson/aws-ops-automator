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
from actions import get_report_output_writer
from actions.action_base import ActionBase
from boto_retry import get_default_retry_strategy
from outputs.report_output_writer import report_key_name

RPT_ACCOUNT = "Account"
RPT_ATTACHED_INSTANCE = "Attached to Instance"
RPT_EARLIEST_SNAPSHOT_ID = "Earliest Snapshot"
RPT_EARLIEST_SNAPSHOT_TIME = "Earliest Snapshot Time"
RPT_ENCRYPTED = "Encrypted"
RPT_IOPS = "Iops"
RPT_MOST_RECENT_SNAPSHOT_ID = "Most Recent Snapshot"
RPT_MOST_RECENT_SNAPSHOT_TIME = "Most Recent Snapshot Time"
RPT_NUMBER_OF_SNAPSHOTS = "Number of Snapshots"
RPT_PROGRESS = "Progress"
RPT_SIZE = "Size (GiB)"
RPT_SNAPSHOT = "Created from Snapshot"
RPT_STATUS = "Status"
RPT_VOLUME_ID = "Volume ID"
RPT_VOLUME_NAME = "Volume Name"
RPT_VOLUME_TYPE = "Volume Type"
RPT_ATTACHED_DEVICE = "Device"
RPT_REGION = "Region"

CSV_COLUMNS = [
    RPT_ACCOUNT,
    RPT_REGION,
    RPT_VOLUME_ID,
    RPT_VOLUME_TYPE,
    RPT_IOPS,
    RPT_SIZE,
    RPT_SNAPSHOT,
    RPT_ATTACHED_INSTANCE,
    RPT_ATTACHED_DEVICE,
    RPT_ENCRYPTED,
    RPT_NUMBER_OF_SNAPSHOTS,
    RPT_EARLIEST_SNAPSHOT_TIME,
    RPT_EARLIEST_SNAPSHOT_ID,
    RPT_MOST_RECENT_SNAPSHOT_TIME,
    RPT_MOST_RECENT_SNAPSHOT_ID,
    RPT_STATUS,
    RPT_PROGRESS
]

ERR_REPORT_ACCOUNT_NOT_UNIQUE = "Task processes more than one account but name template does not contain {} or {}, so output " \
                                "file for different regions is not unique"
ERR_REPORT_REGION_NOT_UNIQUE = "Task processes more than one region but name template does not contain {} or {}, so output " \
                               "file for different regions is not unique"

SNAPSHOT_SELECT = "Snapshots[*].{SnapshotId: SnapshotId,VolumeId: VolumeId, Encrypted:Encrypted, StartTime:StartTime, " \
                  "State:State,Progress:Progress}"

VOLUME_TYPES_MAP = {u'standard': u'Standard/Magnetic', u'io1': u'Provisioned IOPS (SSD)', u'gp2': u'General Purpose SSD',
                    u'sc1': u'Cold HDD', u'st1': u'Throughput Optimized HDD'}

GROUP_TITLE_REPORT_OPTIONS = "Report options"

PARAM_DESC_REPORT_NAME = "Name of the report"
PARAM_DESC_WRITE_HEADERS = "Write header row to CSV output file"
PARAM_DESC_ATTACHED_ONLY = "Include only attached EBS volumes in reports"
PARAM_DESC_REPORT_TIMEZONE = "Timezone to use for datetime values in output report"
PARAM_DESC_FORMAT = "Select CSV or JSON output format for report"

PARAM_LABEL_REPORT_NAME = "Report name"
PARAM_LABEL_WRITE_HEADERS = "Write headers"
PARAM_LABEL_ATTACHED_ONLY = "Attached volumes only"
PARAM_LABEL_REPORT_TIMEZONE = "Report timezone"
PARAM_LABEL_FORMAT = "Report format"

PARAM_ATTACHED_ONLY = "AttachedOnly"


class Ec2ReportVolumeSnapshotsAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Report Volume Snapshots",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Generates report for snapshots created for EBS volumes",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "c260c938-667e-486b-8b27-79b2cf5a8a60",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.VOLUMES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: True,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_EXECUTE_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_PARAMETERS: {
            PARAM_ATTACHED_ONLY: {
                PARAM_DESCRIPTION: PARAM_DESC_ATTACHED_ONLY,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_ATTACHED_ONLY
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_REPORT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ATTACHED_ONLY
                ],
            }],

        ACTION_PERMISSIONS: ["ec2:DescribeSnapshots"],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.attached_only = self.get(PARAM_ATTACHED_ONLY, False)

        self.volumes = self._resources_
        self._report_writer = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    def _get_snapshots(self):
        volumed_ids = [i["VolumeId"] for i in self.volumes]

        # create service instance to get snapshots
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        snapshots_for_volumes = {}
        last_volume_id = None
        last_snapshot = None
        snapshots_for_volume_count = 0

        # get snapshots for selected volumes
        snapshots = ec2.describe(services.ec2_service.SNAPSHOTS, OwnerIds=["self"],
                                 region=self._region_, select=SNAPSHOT_SELECT,
                                 Filters=[
                                     {
                                         "Name": "volume-id",
                                         "Values": volumed_ids
                                     }
                                 ])

        for snapshot in sorted(snapshots, key=lambda s: (s["VolumeId"], s["StartTime"])):
            if snapshot["VolumeId"] != last_volume_id:
                if last_volume_id is not None:
                    snapshots_for_volumes[last_volume_id]["Last"] = last_snapshot
                    snapshots_for_volumes[last_volume_id]["Count"] = snapshots_for_volume_count
                last_volume_id = snapshot["VolumeId"]
                snapshots_for_volume_count = 0
                snapshots_for_volumes[last_volume_id] = {"First": snapshot}
            snapshots_for_volume_count += 1
            last_snapshot = snapshot
        if last_snapshot is not None:
            snapshots_for_volumes[last_volume_id]["Last"] = last_snapshot
            snapshots_for_volumes[last_volume_id]["Count"] = snapshots_for_volume_count

        return snapshots_for_volumes

    @property
    def report_writer(self):
        if self._report_writer is None:
            self._report_writer = get_report_output_writer(context=self._context_, logger=self._logger_)
        return self._report_writer

    def _volume_report_data(self, snapshot_data):

        for volume in self.volumes:
            attachments = volume.get("Attachments", [])

            if len(attachments) == 0 and self.attached_only:
                continue

            volume_id = volume["VolumeId"]

            output = {
                RPT_ACCOUNT: volume["AwsAccount"],
                RPT_REGION: volume["Region"],
                RPT_VOLUME_ID: volume_id,
                RPT_VOLUME_NAME: volume["Tags"].get("Name", ""),
                RPT_VOLUME_TYPE: VOLUME_TYPES_MAP.get(volume["VolumeType"], "unknown"),
                RPT_IOPS: volume.get("Iops", 0),
                RPT_SIZE: volume["Size"],
                RPT_SNAPSHOT: volume.get("SnapshotId", ""),
                RPT_ATTACHED_INSTANCE: attachments[0]["InstanceId"] if len(attachments) != 0 else "",
                RPT_ATTACHED_DEVICE: attachments[0]["Device"] if len(attachments) != 0 else "",
                RPT_ENCRYPTED: "yes" if volume["Encrypted"] else "no"
            }

            if volume_id in snapshot_data:
                volume_snapshot_data = {
                    RPT_NUMBER_OF_SNAPSHOTS: snapshot_data[volume_id]["Count"],
                    RPT_EARLIEST_SNAPSHOT_TIME:
                        str(snapshot_data[volume_id]["First"]["StartTime"]).split("+")[0],
                    RPT_EARLIEST_SNAPSHOT_ID: snapshot_data[volume_id]["First"]["SnapshotId"],
                    RPT_MOST_RECENT_SNAPSHOT_TIME:
                        str(snapshot_data[volume_id]["Last"]["StartTime"]).split("+")[0],
                    RPT_MOST_RECENT_SNAPSHOT_ID: snapshot_data[volume_id]["Last"]["SnapshotId"],
                    RPT_STATUS: snapshot_data[volume_id]["Last"]["State"],
                    RPT_PROGRESS: snapshot_data[volume_id]["Last"]["Progress"]
                }
            else:
                volume_snapshot_data = {
                    RPT_NUMBER_OF_SNAPSHOTS: 0,
                    RPT_EARLIEST_SNAPSHOT_TIME: "",
                    RPT_EARLIEST_SNAPSHOT_ID: "",
                    RPT_MOST_RECENT_SNAPSHOT_TIME: "",
                    RPT_MOST_RECENT_SNAPSHOT_ID: "",
                    RPT_STATUS: "",
                    RPT_PROGRESS: ""
                }

            output.update(volume_snapshot_data)

            yield output

    def _build_csv_report(self, data):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)
        csv_data.writerow(CSV_COLUMNS)

        for volume in data:
            csv_data.writerow([volume.get(column_name, "") for column_name in CSV_COLUMNS])

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def execute(self):

        self._logger_.info('Generating EBS volume snapshot report for account {} in region {}', self._account_, self._region_)
        snapshots = self._get_snapshots()
        volume_snapshot_data = self._volume_report_data(snapshots)

        self._build_csv_report(volume_snapshot_data)

        print(list(volume_snapshot_data))
        self.result.update({
            "volumes": len(self.volumes),
            METRICS_DATA: build_action_metrics(self, ReportedVolumes=len(self.volumes))

        })

        return self.result
