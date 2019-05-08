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

import services.redshift_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_CLUSTER_ID = "cluster-id"
TAG_PLACEHOLDER_SNAPSHOT_ID = "snapshot-id"

GROUP_TITLE_TAGGING_NAMING = "Tagging and naming options"

PARAM_DESC_ACCOUNTS_RESTORE_ACCESS = "Comma separated list of accounts that will be granted access to restore the snapshot"
PARAM_DESC_COPIED_CLUSTER_TAGS = "Copied tags from cluster to snapshot"
PARAM_DESC_PREFIX = "Prefix for snapshot"
PARAM_DESC_SNAPSHOT_TAGS = "Tags to add to snapshot, use list of tagname=tagvalue pairs"
PARAM_DESC_SNAPSHOT_NAME = "Name of the created snapshot, leave blank for default snapshot name"
PARAM_DESC_CLUSTER_TAGS = "Tags to add to cluster of snapshot is created, use list of tagname=tagvalue pairs"

PARAM_LABEL_ACCOUNTS_RESTORE_ACCESS = "Grant restore access to accounts"
PARAM_LABEL_COPIED_CLUSTER_TAGS = "Copied cluster tags"
PARAM_LABEL_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_SNAPSHOT_NAME = "Snapshot name"
PARAM_LABEL_CLUSTER_TAGS = "Cluster tags"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

INFO_CREATE_SNAPSHOT = "Creating snapshot for redshift cluster \"{}\""
INFO_SNAPSHOT_CREATED = "Snapshot is {}"
INFO_SNAPSHOT_NAME = "Name of the snapshot is {}"
INFO_START_SNAPSHOT_ACTION = "Creating snapshot for redshift cluster \"{}\" for task \"{}\""
INFO_GRANT_ACCOUNT_ACCESS = "Granted access to snapshot for account {}"
INF_SNAPSHOT_DATA = "Snapshot data is {}"
INF_NOT_YET_CREATED = "Snapshot \"{}\" does has not been created yet"
INF_SNAPSHOT_NOT_COMPLETED = "Snapshot not completed yet"

ERR_CLUSTER_NOT_AVAILABLE = "Status of cluster is \"{}\", can only make snapshot of cluster with status \"available\""
ERR_SNAPSHOT_FAILED = "Creation of snapshot for cluster \"{}\" failed"

PARAM_ACCOUNTS_RESTORE_ACCESS = "AccountsWithRestoreAccess"
PARAM_COPIED_CLUSTER_TAGS = "CopiedInstanceTags"
PARAM_PREFIX = "Prefix"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_SNAPSHOT_NAME = "SnapshotName"
PARAM_CLUSTER_TAGS = "ClusterTags"


class RedshiftCreateSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "RedShift Create Snapshot",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPTION: "Creates snapshot (manual type) for Redshift cluster",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6310b757-d8a8-4031-af29-29b9fc5bcf65",

        ACTION_SERVICE: "redshift",
        ACTION_RESOURCES: services.redshift_service.CLUSTERS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_EXPRESSION: "Clusters[*].{ClusterIdentifier:ClusterIdentifier,ClusterStatus:ClusterStatus,Tags:Tags}",

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_PARAMETERS: {

            PARAM_COPIED_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_CLUSTER_TAGS
            },
            PARAM_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_PREFIX,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_PREFIX
            },
            PARAM_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_CLUSTER_TAGS
            },
            PARAM_ACCOUNTS_RESTORE_ACCESS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_RESTORE_ACCESS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_RESTORE_ACCESS
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SNAPSHOT_NAME,
                    PARAM_PREFIX,
                    PARAM_CLUSTER_TAGS,
                    PARAM_COPIED_CLUSTER_TAGS,
                    PARAM_SNAPSHOT_TAGS
                ],
            }],

        ACTION_PERMISSIONS: [
            "redshift:DescribeClusters",
            "redshift:CreateClusterSnapshot",
            "redshift:DescribeClusterSnapshots",
            "redshift:DescribeTags",
            "redshift:CreateTags",
            "redshift:DeleteTags",
            "redshift:AuthorizeSnapshotAccess"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.cluster = self._resources_

        self.cluster_id = self.cluster["ClusterIdentifier"]
        self.cluster_tags = self.cluster.get("Tags", {})
        self.cluster_status = self.cluster["ClusterStatus"]

        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_CLUSTER_TAGS, ""))

        self.granted_accounts = self.get(PARAM_ACCOUNTS_RESTORE_ACCESS, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "cluster-identifier": self.cluster_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        cluster = arguments[ACTION_PARAM_RESOURCES]
        account = cluster["AwsAccount"]
        region = cluster["Region"]
        cluster_id = cluster["ClusterIdentifier"]
        return "{}-{}-{}-{}".format(account, region, cluster_id, log_stream_date())

    def is_completed(self, snapshot_create_data):

        def set_cluster_tags(snap_id):
            tags = self.build_tags_from_template(parameter_name=PARAM_CLUSTER_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_SNAPSHOT_ID: snap_id
                                                 })

            client = get_client_with_retries("redshift",
                                             methods=[
                                                 "create_tags",
                                                 "delete_tags"
                                             ],
                                             context=self._context_,
                                             session=self._session_,
                                             logger=self._logger_)

            arn = "arn:aws-us-gov:redshift:{}:{}:cluster:{}".format(self._region_, self._account_, self.cluster_id)
            tagging.set_redshift_tags(redshift_client=client,
                                      tags=tags,
                                      can_delete=True,
                                      logger=self._logger_,
                                      resource_arns=[arn])

        self._logger_.debug("Start result data is {}", safe_json(snapshot_create_data, indent=3))

        snapshot_id = snapshot_create_data.get("snapshot-identifier", None)

        self._logger_.info("Checking status of snapshot \"{}\"for cluster \"{}\"", snapshot_id, self.cluster_id)

        # create service instance to test is snapshots are available
        redshift = services.create_service("redshift", session=self._session_,
                                           service_retry_strategy=get_default_retry_strategy("redshift", context=self._context_))

        # test if the snapshot with the id that was returned from the CreateSnapshot API call exists and is available
        snapshot = redshift.get(services.redshift_service.CLUSTER_SNAPSHOTS,
                                SnapshotIdentifier=snapshot_id,
                                region=self._region_)

        if snapshot is None:
            self._logger_.info(INF_NOT_YET_CREATED, snapshot_id)
            return None

        self._logger_.info(INF_SNAPSHOT_DATA, safe_json(snapshot, indent=3))
        status = snapshot["Status"]

        if status == "failed":
            raise_exception(ERR_SNAPSHOT_FAILED, self.cluster_id)

        if status == "available":
            self.result["total-backup-size"] = snapshot["TotalBackupSizeInMegaBytes"]
            self.result["actual-incremental-backup-size"] = snapshot["ActualIncrementalBackupSizeInMegaBytes"]
            set_cluster_tags(snapshot_id)
            return self.result

        self._logger_.info(INF_SNAPSHOT_NOT_COMPLETED)
        return None

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INFO_START_SNAPSHOT_ACTION, self.cluster_id, self._task_)

        if self.cluster_status != "available":
            raise_exception(ERR_CLUSTER_NOT_AVAILABLE, self.cluster_status)

        tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.cluster_tags)
        tags.update(
            self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                          tag_variables={TAG_PLACEHOLDER_CLUSTER_ID: self.cluster_id}))

        snapshot_name = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME,
                                                     tag_variables={TAG_PLACEHOLDER_CLUSTER_ID: self.cluster_id})

        if snapshot_name == "":
            dt = self._datetime_.utcnow()

            snapshot_name = SNAPSHOT_NAME.format(self.cluster_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

        prefix = self.build_str_from_template(parameter_name=PARAM_PREFIX,
                                              tag_variables={TAG_PLACEHOLDER_CLUSTER_ID: self.cluster_id})

        snapshot_name = prefix + snapshot_name

        redshift = get_client_with_retries("redshift",
                                           methods=[
                                               "create_cluster_snapshot",
                                               "authorize_snapshot_access"
                                           ],
                                           context=self._context_,
                                           session=self._session_,
                                           logger=self._logger_)

        create_snapshot_resp = redshift.create_cluster_snapshot_with_retries(SnapshotIdentifier=snapshot_name,
                                                                             Tags=tag_key_value_list(tags),
                                                                             ClusterIdentifier=self.cluster_id)
        self.result["snapshot-identifier"] = snapshot_name
        self.result["snapshot-create-time"] = create_snapshot_resp["Snapshot"]["SnapshotCreateTime"]
        self._logger_.info(INFO_SNAPSHOT_CREATED, snapshot_name)

        if self.granted_accounts is not None and len(self.granted_accounts) > 0:
            for account in self.granted_accounts:
                redshift.authorize_snapshot_access_with_retries(SnapshotIdentifier=snapshot_name,
                                                                SnapshotClusterIdentifier=self.cluster_id,
                                                                AccountWithRestoreAccess=account)

                self._logger_.info(INFO_GRANT_ACCOUNT_ACCESS, account)
            self.result["granted-access-accounts"] = self.granted_accounts

        self.result[METRICS_DATA] = build_action_metrics(self, CreatedSnapshots=1)

        return self.result
