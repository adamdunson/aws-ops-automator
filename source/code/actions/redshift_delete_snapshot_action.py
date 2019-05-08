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

from datetime import timedelta

import dateutil.parser
from botocore.exceptions import ClientError

import pytz
import services.redshift_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_value_error

INFO_REVOKE_ACCESS = "Revoking restore access for account {}"

INFO_DELETE_SNAPSHOT = "Deleting snapshot {} for cluster {}"

GROUP_TITLE_DELETE_OPTIONS = "Snapshot delete options"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for a RedShift cluster, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_SNAPSHOTS = "{} cluster snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each Redshift cluster"
INFO_REGION = "Processing snapshots in region {}"
INFO_RETENTION_DAYS = "Deleting snapshots older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting snapshot {}, because count for its volume is {}"
INFO_SN_RETENTION_DAYS = "Deleting snapshot {} ({}) because it is older than retention period of {} days"
INFO_SNAPSHOT_DELETED = "Deleted snapshot {} for volume {}"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class RedshiftDeleteSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "RedShift Delete Snapshot",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPTION: "Deletes Redshift snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "2fb2442c-b847-4dab-b53e-e481e029cc30f",

        ACTION_SERVICE: "redshift",
        ACTION_RESOURCES: services.redshift_service.CLUSTER_SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "Snapshots[?Status=='available']|[?SnapshotType=='manual']."
                                  "{SnapshotIdentifier:SnapshotIdentifier, "
                                  "ClusterIdentifier:ClusterIdentifier,"
                                  "SnapshotCreateTime:SnapshotCreateTime,"
                                  "AccountsWithRestoreAccess:AccountsWithRestoreAccess[*].AccountId}",

        ACTION_KEEP_RESOURCE_TAGS: False,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_PARAMETERS: {
            PARAM_RETENTION_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_DAYS,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_DAYS
            },
            PARAM_RETENTION_COUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_COUNT,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_COUNT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_DELETE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_RETENTION_DAYS,
                    PARAM_RETENTION_COUNT

                ],
            }],

        ACTION_PERMISSIONS: [
            "redshift:DescribeClusterSnapshots",
            "redshift:DeleteClusterSnapshot",
            "redshift:RevokeSnapshotAccess"
        ]

    }

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        retention_days = parameters.get(PARAM_RETENTION_DAYS)
        retention_count = parameters.get(PARAM_RETENTION_COUNT)
        if not retention_count and not retention_days:
            raise_value_error(ERR_RETENTION_PARAM_NONE, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        if retention_days and retention_count:
            raise_value_error(ERR_RETENTION_PARAM_BOTH, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        return parameters

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.snapshots = sorted(self._resources_)
        self.retention_days = int(self.get(PARAM_RETENTION_DAYS))
        self.retention_count = int(self.get(PARAM_RETENTION_COUNT))

        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "deleted-snapshots": []
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        retention_count = int(arguments["event"][ACTION_PARAMETERS].get(PARAM_RETENTION_COUNT, 0))
        if retention_count == 0:
            return "{}-{}-{}".format(account, region, log_stream_date())
        else:
            return "{}-{}-{}-{}".format(account, region, arguments[ACTION_PARAM_RESOURCES][0].get("SnapshotIdentifier", ""),
                                        log_stream_date())

    def execute(self):

        def get_creation_time(s):
            if isinstance(s["SnapshotCreateTime"], datetime):
                return s["SnapshotCreateTime"]
            return dateutil.parser.parse(s["SnapshotCreateTime"])

        def snapshots_to_delete():

            def by_retention_days():

                delete_before_dt = self._datetime_.utcnow().replace(tzinfo=pytz.timezone("UTC")) - timedelta(
                    days=int(self.retention_days))
                self._logger_.info(INFO_RETENTION_DAYS, delete_before_dt)

                for sn in sorted(self.snapshots, key=lambda s: s["Region"]):
                    snapshot_dt = get_creation_time(sn)
                    if snapshot_dt < delete_before_dt:
                        self._logger_.info(INFO_SN_RETENTION_DAYS, sn["SnapshotIdentifier"], get_creation_time(sn),
                                           self.retention_days)
                        yield sn

            def by_retention_count():

                self._logger_.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda s: (s["ClusterIdentifier"], get_creation_time(s)),
                                          reverse=True)
                cluster = None
                count_for_cluster = 0
                for sn in sorted_snapshots:
                    if sn["ClusterIdentifier"] != cluster:
                        cluster = sn["ClusterIdentifier"]
                        count_for_cluster = 0

                    count_for_cluster += 1
                    if count_for_cluster > self.retention_count:
                        self._logger_.info(INFO_SN_DELETE_RETENTION_COUNT, sn["SnapshotIdentifier"], count_for_cluster)
                        yield sn

            return by_retention_days() if self.retention_days else by_retention_count()

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        deleted_count = 0

        self._logger_.info(INFO_ACCOUNT_SNAPSHOTS, len(self.snapshots), self._account_)

        self._logger_.debug("Cluster Snapshots : {}", self.snapshots)

        redshift = get_client_with_retries("redshift",
                                           methods=[
                                               "delete_cluster_snapshot",
                                               "revoke_snapshot_access"],
                                           region=self._region_,
                                           context=self._context_,
                                           session=self._session_,
                                           logger=self._logger_)

        for snapshot in (snapshots_to_delete()):

            if self.time_out():
                break

            snapshot_id = ""
            try:
                snapshot_id = snapshot["SnapshotIdentifier"]
                cluster_id = snapshot["ClusterIdentifier"]
                granted_accounts = snapshot.get("AccountsWithRestoreAccess", [])
                if granted_accounts is None:
                    granted_accounts = []

                self._logger_.info(INFO_DELETE_SNAPSHOT, snapshot_id, cluster_id)
                for account in granted_accounts:
                    self._logger_.info(INFO_REVOKE_ACCESS, account)
                    redshift.revoke_snapshot_access_with_retries(SnapshotIdentifier=snapshot_id,
                                                                 SnapshotClusterIdentifier=cluster_id,
                                                                 AccountWithRestoreAccess=account)

                redshift.delete_cluster_snapshot_with_retries(SnapshotIdentifier=snapshot_id,
                                                              SnapshotClusterIdentifier=cluster_id)
                self._logger_.info(INFO_SNAPSHOT_DELETED, snapshot_id, cluster_id)
                self.result["deleted-snapshots"].append(snapshot_id)

            except ClientError as ex_client:
                if ex_client.response.get("Error", {}).get("Code", "") == "ClusterSnapshotNotFound":
                    self._logger_.info("Snapshot \"{}\" was not found and could not be deleted", snapshot_id)
                else:
                    raise ex_client

            except Exception as ex:
                if self.dryrun:
                    self._logger_.debug(str(ex))
                    self.result["delete_cluster_snapshot"] = str(ex)
                    return self.result

                else:
                    raise ex

        self.result.update({
            "snapshots": len(self.snapshots)
        })

        self.result[METRICS_DATA] = build_action_metrics(self, DeletedSnapshots=1)

        return self.result
