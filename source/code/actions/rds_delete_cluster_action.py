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

import boto_retry
import handlers.rds_tag_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_default_retry_strategy
from helpers import safe_json
from helpers.timer import Timer
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_CLUSTER_ID = "db-cluster-id"
TAG_PLACEHOLDER_DB_NAME = "db-name"
TAG_PLACEHOLDER_ENGINE = "db-engine"
TAG_PLACEHOLDER_ENGINE_VERSION = "db-engine-version"

INF_DELETING_CLUSTER_FOR_TASK = "Deleting RDS cluster {} for task {}"
INF_FINAL_SNAPSHOT = "A final snapshot {} will be created for RDS cluster {}"
INF_GRANTING_RESTORE_PERMISSION = "Granting restore permissions to accounts {}"
INF_CLUSTER_ALREADY_DELETED = "RDS cluster {} already being deleted by task {}"
INF_CLUSTER_DELETED = "RDS cluster is {} deleted"
INF_CLUSTER_AVAILABLE = "Cluster {} is available for delete"
INF_CLUSTER_STATE = "Status of cluster {} is {}"
INF_NO_SNAPSHOT_YET = "No final snapshot {} yet"
INF_SETTING_FINAL_SNAPSHOT_TAGS = "Setting tags {} to final snapshot {}"
INF_START_CHECKING_STATUS_OF_CLUSTER = "Checking status of cluster {}, status is {}"
INF_STARTING_STOPPED_CLUSTER = "RDS cluster {} is not running, starting in order to delete it"
INF_TERMINATION_COMPLETED = "Termination of RDS cluster {} completed"
INF_TERMINATION_COMPLETED_WITH_SNAPSHOT = "Termination of RDS cluster {} completed, snapshot {} is created and available"
INF_WAITING_FOR_CLUSTER_AVAILABLE = "Waiting for RDS cluster {} to become available to delete it"
INF_CREATING_FINAL_SNAPSHOT_PROGRESS = "Creating final snapshot {} for cluster {}, progress is {}%"
INF_STOP_STARTED_CLUSTER = "Cluster {} could not be deleted, cluster was started for deletion and will be stopped."

ERR_FINAL_SNAPSHOT_FAILED = "Error creating final snapshot {} for cluster {}"
ERR_CLUSTER_IS_STOPPED = "Cannot create a final snapshot because RDS cluster {} is not available, status is {}"
ERR_SETTING_FINAL_SNAPSHOT_TAGS = "Error setting tags to final snapshot {}, {}"
ERR_SETTING_RESTORE_PERMISSION = "Error granting restore permissions for last snapshot to accounts {}, {}"
ERR_STARTED_DB_CLUSTER_NO_LONGER_EXISTS = "RDS cluster {} does not longer exists"
ERR_STARTING_STOPPED_CLUSTER_FOR_SNAPSHOT = "Error starting stopped RDS cluster for deletion"

CLUSTER_STATUS_AVAILABLE = "available"
CLUSTER_STATUS_STARTING = "starting"
CLUSTER_STATUS_STOPPED = "stopped"
CLUSTER_STATUS_DELETING = "deleting"

SNAPSHOT_STATUS_CREATING = "creating"
SNAPSHOT_STATUS_AVAILABLE = "available"
SNAPSHOT_STATUS_FAILED = "failed"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

PARAM_CREATE_SNAPSHOT = "CreateSnapshot"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_NAME = "SnapshotName"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_COPIED_CLUSTER_TAGS = "CopiedClusterTags"
PARAM_RESTORE_PERMISSION = "GrantRestorePermission"
PARAM_START_STOPPED_TO_DELETE = "StartStopped"

PARAM_DESC_CREATE_SNAPSHOT = "Creates a final snapshot before deleting the RDS cluster."
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to the created final snapshot. Note that tag values for RDS cannot contain ',' characters. When specifying " \
    "multiple follow up tasks in the value of the Ops Automator task list tag use  a '/' character instead"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for name final snapshot."
PARAM_DESC_SNAPSHOT_NAME = "Name of the final snapshot, leave blank for default snapshot name"
PARAM_DESC_RESTORE_PERMISSION = "Accounts authorized to copy or restore the RDS snapshot"
PARAM_DESC_COPIED_CLUSTER_TAGS = \
    "Enter a tag filter to copy tags from the RDS cluster to the final snapshot. For example, enter * to copy all tags " \
    "from the RDS cluster to the snapshot."
PARAM_DESC_START_STOPPED_TO_DELETE = \
    "In order to delete the cluster it should be available. Set Start stopped cluster to delete it."

PARAM_LABEL_CREATE_SNAPSHOT = "Create final snapshot"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Final snapshot name prefix"
PARAM_LABEL_SNAPSHOT_NAME = "Final snapshot name"
PARAM_LABEL_SNAPSHOT_TAGS = "Final snapshot tags"
PARAM_LABEL_COPIED_CLUSTER_TAGS = "Copied RDS cluster tags"
PARAM_LABEL_RESTORE_PERMISSION = "Accounts with restore permissions"
PARAM_LABEL_START_STOPPED_TO_DELETE = "Allow starting stopped cluster"

GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot options"


class RdsDeleteClusterAction(ActionBase):
    """
    Implements action to delete a RDS Cluster with an optional final snapshot
    """
    properties = {
        ACTION_TITLE: "RDS Delete Cluster",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes RDS cluster with optional snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "25ef8736-ca90-431a-b504-74513757351f ",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_CLUSTERS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "DBClusters[].{DBClusterIdentifier:DBClusterIdentifier," +
            "Status:Status, " +
            "DBClusterArn:DBClusterArn, " +
            "DBName:DBName, " +
            "Engine:Engine," +
            "EngineVersion:EngineVersion}" +
            "|[?contains(['stopped','available','creating','stopping','modifying','backing-up'],Status)]",

        ACTION_SELECTION_REQUIRES_TAGS: True,

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_CLUSTER_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {
            PARAM_COPIED_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_CLUSTER_TAGS
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_SNAPSHOT_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME_PREFIX
            },
            PARAM_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME
            },
            PARAM_CREATE_SNAPSHOT: {
                PARAM_DESCRIPTION: PARAM_DESC_CREATE_SNAPSHOT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_CREATE_SNAPSHOT
            },
            PARAM_START_STOPPED_TO_DELETE: {
                PARAM_DESCRIPTION: PARAM_DESC_START_STOPPED_TO_DELETE,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_START_STOPPED_TO_DELETE
            },
            PARAM_RESTORE_PERMISSION: {
                PARAM_DESCRIPTION: PARAM_DESC_RESTORE_PERMISSION,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_RESTORE_PERMISSION
            }
        },
        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CREATE_SNAPSHOT,
                    PARAM_START_STOPPED_TO_DELETE,
                    PARAM_COPIED_CLUSTER_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_RESTORE_PERMISSION
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:DeleteDBCluster",
            "rds:DeleteDBInstance",
            "rds:AddTagsToResource",
            "rds:DescribeDBClusters",
            "rds:DescribeDBClusterSnapshots",
            "rds:DescribeDBInstances",
            "rds:ModifyDBClusterSnapshotAttribute",
            "rds:RemoveTagsFromResource",
            "rds:StartDBCluster",
            "rds:ListTagsForResource",
            "rds:StopDBCluster",
            "tag:GetResources"]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_cluster = self._resources_

        self.db_cluster_id = self.db_cluster["DBClusterIdentifier"]
        self.db_cluster_arn = self.db_cluster["DBClusterArn"]
        self._rds_client = None
        self._rds_service = None

        self.create_snapshot = self.get(PARAM_CREATE_SNAPSHOT, True)
        self.start_stopped_cluster = self.get(PARAM_START_STOPPED_TO_DELETE, True)

        # tags from the RDS cluster
        self.cluster_tags = self.db_cluster.get("Tags", {})
        # filter for tags copied from RDS  cluster to image
        self.copied_cluster_tagfilter = TagFilterSet(self.get(PARAM_COPIED_CLUSTER_TAGS, ""))

        self.accounts_with_restore_permissions = self.get(PARAM_RESTORE_PERMISSION, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "db-cluster": self.db_cluster_id,
            "task": self._task_
        }

    @property
    def rds_client(self):

        if self._rds_client is None:
            methods = [
                "delete_db_cluster",
                "delete_db_instance",
                "describe_db_clusters",
                "add_tags_to_resource",
                "describe_db_cluster_snapshots",
                "modify_db_cluster_snapshot_attribute",
                "remove_tags_from_resource",
                "start_db_cluster",
                "stop_db_cluster"
            ]

            self._rds_client = boto_retry.get_client_with_retries("rds",
                                                                  methods,
                                                                  region=self._region_,
                                                                  session=self._session_,
                                                                  context=self._context_,
                                                                  logger=self._logger_)

        return self._rds_client

    @property
    def rds_service(self):
        if self._rds_service is None:
            self._rds_service = services.create_service("rds", session=self._session_,
                                                        service_retry_strategy=get_default_retry_strategy("rds",
                                                                                                          context=self._context_))
        return self._rds_service

    @staticmethod
    def filter_resource(rds_inst):
        return rds_inst["Status"] in [
            "stopped",
            "available",
            "creating",
            "stopping",
            "creating",
            "modifying",
            "backing-up"]

    @staticmethod
    def action_logging_subject(arguments, _):
        db_cluster = arguments[ACTION_PARAM_RESOURCES]
        db_cluster_id = db_cluster["DBClusterIdentifier"]
        account = db_cluster["AwsAccount"]
        region = db_cluster["Region"]
        return "{}-{}-{}-{}".format(account, region, db_cluster_id, log_stream_date())

    def is_completed(self, exec_result):

        def grant_restore_permissions(snap_id):

            if self.accounts_with_restore_permissions is not None and len(self.accounts_with_restore_permissions) > 0:

                args = {
                    "DBClusterSnapshotIdentifier": snap_id,
                    "AttributeName": "restore",
                    "ValuesToAdd": [a.strip() for a in self.accounts_with_restore_permissions]
                }

                try:
                    self.rds_client.modify_db_cluster_snapshot_attribute_with_retries(**args)
                    self._logger_.info(INF_GRANTING_RESTORE_PERMISSION, ", ".join(self.accounts_with_restore_permissions))
                    self.result["restore-access-accounts"] = [a.strip() for a in self.accounts_with_restore_permissions]
                except Exception as grant_ex:
                    raise_exception(ERR_SETTING_RESTORE_PERMISSION, self.accounts_with_restore_permissions, grant_ex)

        def set_tags_to_final_snapshot(snapshot, cluster_tags):

            # tags on the snapshot
            tags = snapshot.get("Tags", {})

            tags.update(self.copied_cluster_tagfilter.pairs_matching_any_filter(cluster_tags))

            tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_CLUSTER_ID: self.db_cluster_id,
                                                  TAG_PLACEHOLDER_DB_NAME:
                                                      self.db_cluster.get("DBName", "")
                                                      if self.db_cluster.get("DBName", "") is not None else "",
                                                  TAG_PLACEHOLDER_ENGINE: self.db_cluster.get("Engine", ""),
                                                  TAG_PLACEHOLDER_ENGINE_VERSION: self.db_cluster.get("EngineVersion", ""),
                                              },
                                              restricted_value_set=True))

            if len(tags) > 0:
                try:
                    self._logger_.info(INF_SETTING_FINAL_SNAPSHOT_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                       snapshot["DBClusterSnapshotArn"])
                    tagging.set_rds_tags(rds_client=self.rds_client,
                                         resource_arns=[snapshot["DBClusterSnapshotArn"]],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.flush()

                except Exception as tag_ex:
                    raise_exception(ERR_SETTING_FINAL_SNAPSHOT_TAGS, snapshot["DBClusterSnapshotArn"], tag_ex)

        def get_final_snapshot(snap_id):

            return self.rds_service.get(services.rds_service.DB_CLUSTER_SNAPSHOTS,
                                        region=self.db_cluster["Region"],
                                        tags=False,
                                        DBClusterSnapshotIdentifier=snap_id,
                                        _expected_boto3_exceptions_=["DBClusterSnapshotNotFoundFault"])

        def get_deleted_cluster():

            try:
                return self.rds_service.get(services.rds_service.DB_CLUSTERS,
                                            region=self._region_,
                                            DBClusterIdentifier=self.db_cluster_id,
                                            _expected_boto3_exceptions_=["DBClusterNotFoundFault"])
            except Exception as rds_ex:
                if type(rds_ex).__name__ == "DBClusterNotFoundFault":
                    return None
                else:
                    raise rds_ex

        cluster = get_deleted_cluster()

        # if cluster was started to delete it, check if it is available
        if cluster is not None:

            cluster_status = cluster["Status"]

            if exec_result.get("stopped-cluster", False):

                # get status of started cluster

                if cluster is not None:

                    self._logger_.info(INF_START_CHECKING_STATUS_OF_CLUSTER, self.db_cluster_id, cluster_status)

                    if cluster_status == CLUSTER_STATUS_STARTING or not self._cluster_members_available():
                        self._logger_.info(INF_WAITING_FOR_CLUSTER_AVAILABLE, self.db_cluster_id)
                        return None

                    if cluster_status == CLUSTER_STATUS_DELETING:
                        self._logger_.info("Cluster is {} deleting", self.db_cluster_id)
                        return None

                    # started cluster is available
                    self._logger_.info(INF_CLUSTER_AVAILABLE, self.db_cluster_id)
                    try:
                        # now it is available, delete the cluster
                        self._delete_cluster(exec_result.get("snapshot-id", None))
                        return None
                    except Exception as ex:
                        self._logger_.info(INF_STOP_STARTED_CLUSTER, self.db_cluster_id)
                        self.rds_client.stop_db_cluster_with_retries(DBClusterIdentifier=self.db_cluster_id)
                        raise ex

        if cluster is not None:
            return None

        # no longer there because it was deleted successfully
        self._logger_.info(INF_CLUSTER_DELETED, self.db_cluster_id)

        # if a snapshot was requested
        if self.create_snapshot:

            # id of the final snapshot
            snapshot_id = exec_result["snapshot-id"]
            self.result["db-snapshot-id"] = snapshot_id

            final_snapshot = get_final_snapshot(snapshot_id)

            # no snapshot yet
            if final_snapshot is None:
                return None

            # check status of final snapshot
            snapshot_status = final_snapshot['Status']

            # failed
            if snapshot_status == SNAPSHOT_STATUS_FAILED:
                raise_exception(ERR_FINAL_SNAPSHOT_FAILED, snapshot_id, self.db_cluster_id)

            # creating but not done yet
            if snapshot_status == SNAPSHOT_STATUS_CREATING:
                progress = final_snapshot.get("PercentProgress", 0)
                self._logger_.info(INF_CREATING_FINAL_SNAPSHOT_PROGRESS, snapshot_id, self.db_cluster_id, progress)
                return None

            # snapshot is available
            if snapshot_status == SNAPSHOT_STATUS_AVAILABLE:
                try:
                    set_tags_to_final_snapshot(final_snapshot, exec_result.get("cluster-tags", {}))
                    grant_restore_permissions(snapshot_id)
                except Exception as ex:
                    raise ex

        return self.result

    def _delete_cluster(self, snapshot_name=None):

        # the task that is used to delete the cluster needs to be deleted from the task list for the cluster
        # if this is not done then an cluster restored from the final snapshot might be deleted because the
        # tag that holds the action list is also restored from the snapshot
        def remove_delete_task_from_tasks_tag():
            # get current cluster tags
            current_cluster_tags = self.db_cluster.get("Tags", {})
            # get tasks
            task_list_tag = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME, "")
            # find task list tag
            if task_list_tag in current_cluster_tags:
                task_list = current_cluster_tags[task_list_tag]
                tasks = tagging.split_task_list(task_list)
                # remove task that is deleting this cluster
                if self._task_ in tasks:
                    tasks = [t for t in tasks if t != self._task_]
                if len(tasks) > 0:
                    # other tags left, update the task list tag
                    self.rds_client.add_tags_to_resource_with_retries(
                        ResourceName=self.db_cluster_arn,
                        Tags=tag_key_value_list({task_list_tag: ",".join(tasks)}))
                else:
                    # no other tasks, delete the tag
                    self.rds_client.remove_tags_from_resource_with_retries(ResourceName=self.db_cluster_arn,
                                                                           TagKeys=[task_list_tag])

        # store original set of tags in case the deletion fails
        cluster_tags = self.db_cluster.get("Tags", {})

        try:
            remove_delete_task_from_tasks_tag()

            members = self._get_cluster_members()

            for instance_member_id in [m["DBInstanceIdentifier"] for m in members if m["DBInstanceStatus"] in "available"]:
                self.rds_client.delete_db_instance_with_retries(DBInstanceIdentifier=instance_member_id)

            with Timer(timeout_seconds=60) as t:
                while True:
                    members = self._get_cluster_members()
                    if len([m["DBInstanceStatus"] for m in members if m["DBInstanceStatus"] != "deleting"]) == 0:
                        break
                    if t.timeout:
                        break

            args = {
                "DBClusterIdentifier": self.db_cluster_id,
            }

            if self.create_snapshot:
                args["FinalDBSnapshotIdentifier"] = snapshot_name
                self._logger_.info(INF_FINAL_SNAPSHOT, snapshot_name, self.db_cluster_id)
            else:
                args["SkipFinalSnapshot"] = True

            args["_expected_boto3_exceptions_"] = [
                "InvalidDBClusterStateFault",
                "DBClusterNotFoundFault"
            ]

            self._logger_.debug("calling delete_db_cluster with arguments {}", safe_json(args, indent=3))
            resp = self.rds_client.delete_db_cluster_with_retries(**args)
            self._logger_.debug("delete_db_cluster response is {}", safe_json(resp, indent=3))
            self._logger_.flush()

        except Exception as ex:
            exception_name = type(ex).__name__
            error = getattr(ex, "response", {}).get("Error", {})
            if exception_name == "InvalidDBClusterStateFault":
                if error.get("Code", "") == "InvalidDBClusterState" and "is already being deleted" in error.get("Message", ""):
                    self._logger_.info(str(ex))
            elif exception_name == "DBCLusterNotFoundFault":
                self._logger_.info(str(ex))
            else:
                self.rds_client.add_tags_to_resource_with_retries(ResourceName=self.db_cluster_arn,
                                                                  Tags=tag_key_value_list(cluster_tags))
                raise Exception("Error deleting RDS cluster {}, {}", self.db_cluster_id, ex)

    def _get_cluster_members(self):
        members = self.rds_service.describe(services.rds_service.DB_INSTANCES,
                                            region=self._region_,
                                            Filters=[
                                                {
                                                    "Name": "db-cluster-id", "Values": [self.db_cluster_id]
                                                }
                                            ])
        return members

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_DELETING_CLUSTER_FOR_TASK, self.db_cluster_id, self._task_)

        status = self.db_cluster["Status"]
        stopped = status == CLUSTER_STATUS_STOPPED
        available = status == CLUSTER_STATUS_AVAILABLE
        self.result["stopped-cluster"] = stopped
        self.result["available"] = available

        # cluster is stopped, if allowed start cluster to delete it
        if stopped:
            if not self.start_stopped_cluster:
                raise_exception(ERR_CLUSTER_IS_STOPPED, self.db_cluster_id, status)
            else:
                self._start_db_cluster()

        if self.create_snapshot:

            final_snapshot_name = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME,
                                                               tag_variables={
                                                                   TAG_PLACEHOLDER_CLUSTER_ID: self.db_cluster_id
                                                               })
            if final_snapshot_name == "":
                dt = self._datetime_.utcnow()
                final_snapshot_name = SNAPSHOT_NAME.format(self.db_cluster_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

            prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                                  tag_variables={
                                                      TAG_PLACEHOLDER_CLUSTER_ID: self.db_cluster_id
                                                  })

            final_snapshot_name = prefix + final_snapshot_name

            self.result["snapshot-id"] = final_snapshot_name
            self.result["cluster-tags"] = self.db_cluster.get("Tags", {})
        else:
            final_snapshot_name = None

        if not stopped and self._cluster_members_available():
            self._delete_cluster(snapshot_name=final_snapshot_name)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            DeletedDBClusters=1
        )

        return self.result

    def _start_db_cluster(self):
        try:
            # start stopped cluster and let completion handler delete the cluster with a final snapshot
            self._logger_.info(INF_STARTING_STOPPED_CLUSTER, self.db_cluster_id)
            self.rds_client.start_db_cluster_with_retries(DBClusterIdentifier=self.db_cluster_id)

        except Exception as ex:
            raise_exception(ERR_STARTING_STOPPED_CLUSTER_FOR_SNAPSHOT, self.db_cluster_id, ex)

    def _cluster_members_available(self):
        members = list(self.rds_service.describe(services.rds_service.DB_INSTANCES,
                                                 region=self._region_,
                                                 Filters=[
                                                     {
                                                         "Name": "db-cluster-id", "Values": [self.db_cluster_id]
                                                     }
                                                 ]))

        return all([m["DBInstanceStatus"] == "available" for m in members])
