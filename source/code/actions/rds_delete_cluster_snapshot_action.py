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

import pytz
import services.rds_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_exception, raise_value_error

GROUP_TITLE_DELETE_OPTIONS = "RDS Cluster Snapshot delete options"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for an RDS cluster, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_SNAPSHOTS = "{} RDS cluster snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each RDS cluster"
INFO_REGION = "Processing cluster snapshots in region {}"
INFO_RETENTION_DAYS = "Deleting RDS cluster snapshots older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting RDS cluster snapshot {}, because count for its cluster is {}"
INFO_SN_RETENTION_DAYS = "Deleting RDS snapshot {} ({}) because it is older than retention period of {} days"
INFO_SNAPSHOT_DELETED = "Deleted RDS snapshot {} for cluster {}"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"
ERR_MAX_RETENTION_COUNT_SNAPSHOTS = "Can not delete if number of snapshots is larger than {} for cluster {}"
ERR_DELETING_SNAPSHOT = "Error deleting snapshot {} for RDS cluster {}, ({})"
INFO_NO_SOURCE_CLUSTER_ID_WITH_RETENTION = \
    "Original cluster id can not be retrieved for snapshot {}, original cluster id is required for " \
    "use with Retention count parameter not equal to 0, snapshot skipped"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class RdsDeleteClusterSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Delete Cluster Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes RDS cluster snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "fe6d9b88-0d2b-4307-8397-eb2a6239714c",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_CLUSTER_SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION: "DBClusterSnapshots[?SnapshotType=='manual']."
                                  "{DBClusterSnapshotIdentifier:DBClusterSnapshotIdentifier, "
                                  "DBClusterIdentifier:DBClusterIdentifier, "
                                  "DBClusterSnapshotArn:DBClusterSnapshotArn, "
                                  "SnapshotCreateTime:SnapshotCreateTime, Status:Status} | [?Status=='available']",

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

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
            "rds:DescribeDBClusterSnapshots",
            "rds:ListTagsForResource",
            "rds:DeleteDBClusterSnapshot",
            "tag:GetResources"
        ]

    }

    # noinspection PyUnusedLocal,PyUnusedLocal
    @staticmethod
    def custom_aggregation(resources, params, logger):

        if params.get(PARAM_RETENTION_COUNT, 0) == 0:
            yield resources
        else:
            snapshots_sorted_by_cluster_id = sorted(resources, key=lambda k: k['DBClusterIdentifier'])
            db_cluster_id = snapshots_sorted_by_cluster_id[0]["DBClusterIdentifier"] if len(
                snapshots_sorted_by_cluster_id) > 0 else None
            snapshots_for_cluster = []
            for snapshot in snapshots_sorted_by_cluster_id:
                if db_cluster_id != snapshot["DBClusterIdentifier"]:
                    yield snapshots_for_cluster
                    db_cluster_id = snapshot["DBClusterIdentifier"]
                    snapshots_for_cluster = [snapshot]
                else:
                    snapshots_for_cluster.append(snapshot)
            yield snapshots_for_cluster

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

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):
        source_db_cluster_id = resource.get("DBClusterIdentifier", None)
        if source_db_cluster_id is None:
            source_db_cluster_id_from_tag = resource.get("Tags", {}).get(
                MARKER_RDS_TAG_SOURCE_DB_CLUSTER_ID.format(os.getenv(handlers.ENV_STACK_NAME)), None)
            if source_db_cluster_id_from_tag is not None:
                resource["DBClusterIdentifier"] = source_db_cluster_id_from_tag
            else:
                if task.get("parameters", {}).get(PARAM_RETENTION_COUNT, 0) > 0:
                    logger.info(INFO_NO_SOURCE_CLUSTER_ID_WITH_RETENTION, resource["SnapshotId"])
                    return None
        return resource

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
            return "{}-{}-{}-{}".format(account, region, arguments[ACTION_PARAM_RESOURCES][0].get("DBClusterIdentifier", ""),
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
                        self._logger_.info(INFO_SN_RETENTION_DAYS, sn["DBClusterSnapshotIdentifier"], get_creation_time(sn),
                                           self.retention_days)
                        yield sn

            def by_retention_count():

                self._logger_.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda s: (s["DBClusterIdentifier"], get_creation_time(s)),
                                          reverse=True)
                db_cluster = None
                count_for_cluster = 0
                for sn in sorted_snapshots:
                    if sn["DBClusterIdentifier"] != db_cluster:
                        db_cluster = sn["DBClusterIdentifier"]
                        count_for_cluster = 0

                    count_for_cluster += 1
                    if count_for_cluster > self.retention_count:
                        self._logger_.info(INFO_SN_DELETE_RETENTION_COUNT, sn["DBClusterSnapshotIdentifier"], count_for_cluster)
                        yield sn

            return by_retention_days() if self.retention_days else by_retention_count()

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        deleted_count = 0

        self._logger_.info(INFO_ACCOUNT_SNAPSHOTS, len(self.snapshots), self._account_)

        self._logger_.debug("Cluster snapshots : {}", self.snapshots)

        cluster_snapshot_id = ""
        db_cluster_id = ""
        for snapshot in list(snapshots_to_delete()):

            if self.time_out():
                break

            rds = get_client_with_retries("rds",
                                          methods=[
                                              "delete_db_cluster_snapshot"
                                          ],
                                          region=self._region_,
                                          context=self._context_,
                                          session=self._session_,
                                          logger=self._logger_)

            try:
                cluster_snapshot_id = snapshot["DBClusterSnapshotIdentifier"]
                db_cluster_id = snapshot["DBClusterIdentifier"]
                rds.delete_db_cluster_snapshot_with_retries(DBClusterSnapshotIdentifier=cluster_snapshot_id,
                                                            _expected_boto3_exceptions_=["DBClusterSnapshotNotFoundFault"])
                deleted_count += 1

                self._logger_.info(INFO_SNAPSHOT_DELETED, cluster_snapshot_id, db_cluster_id)
                self.result["deleted-snapshots"].append(cluster_snapshot_id)
            except Exception as ex:
                exception_name = type(ex).__name__
                if exception_name == "DBClusterSnapshotNotFoundFault":
                    self._logger_.warning(str(ex))
                else:
                    raise_exception(ERR_DELETING_SNAPSHOT, cluster_snapshot_id, db_cluster_id, ex)

        self.result.update({
            "snapshots": len(self.snapshots),
            METRICS_DATA: build_action_metrics(self, DeletedDBClusterSnapshots=deleted_count)

        })

        return self.result
