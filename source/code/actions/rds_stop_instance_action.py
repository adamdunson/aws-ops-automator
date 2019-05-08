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

import handlers.event_handler_base
import handlers.rds_tag_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

SNAPSHOT_TAG = "ops-automator-{}-rds-stop-snapshot"

TAG_PLACEHOLDER_DB_INSTANCE_ID = "db-instance-id"
TAG_PLACEHOLDER_OWNER_ACCOUNT = "owner-account"
TAG_PLACEHOLDER_SOURCE_REGION = "region"
TAG_PLACEHOLDER_SNAPSHOT_ID = "db-snapshot-id"

ERR_SETTING_RESTORE_PERMISSION = "Error granting restore permissions to accounts {}, {}"
ERR_SNAPSHOT_FAILED = "Error creating snapshot {} for instance {}"
ERR_STOP_INSTANCE = "Error stopping instance {}, {}"
ERR_SETTING_SNAPSHOT_TAGS = "Error setting tags to snapshot {}, {}"
ERR_SETTING_INSTANCE_TAGS = "Error setting tags to instance {}, {}"
ERR_INSTANCE_NOT_FOUND = "Stopped instance {} does not longer exist"

INF_CREATING_SNAPSHOT_PROGRESS = "Creating snapshot {} for instance {}, progress is {}%"
INF_GRANTING_RESTORE_PERMISSION = "Granting restore permissions to accounts {}"
INF_INSTANCE_STOP_ACTION = "Stopping RDS instance {} for task {}"
INF_SETTING_SNAPSHOT_TAGS = "Setting tags {} to snapshot {}"
INF_SETTING_INSTANCE_TAGS = "Setting tags {} to instance {}"

SNAPSHOT_STATUS_CREATING = "creating"
SNAPSHOT_STATUS_AVAILABLE = "available"
SNAPSHOT_STATUS_FAILED = "failed"

INSTANCE_STATUS_STOPPED = "stopped"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options"
GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot options"

PARAM_STOPPED_INSTANCE_TAGS = "StoppedInstanceTags"
PARAM_CREATE_SNAPSHOT = "CreateSnapshot"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_NAME = "SnapshotName"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_RESTORE_PERMISSION = "GrantRestorePermission"

PARAM_DESC_STOPPED_INSTANCE_TAGS = "Tags to set on stopped RDS instance."
PARAM_DESC_CREATE_SNAPSHOT = "Creates a snapshot before stopping the RDS instance."
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to the created snapshot. Note that tag values for RDS cannot contain ',' characters. When specifying multiple " \
    "follow up tasks in the value of the Ops Automator task list tag use a '/' character instead"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for name snapshot."
PARAM_DESC_RESTORE_PERMISSION = "Accounts authorized to copy or restore the RDS snapshot"
PARAM_DESC_SNAPSHOT_NAME = "Name of the snapshot, leave blank for default snapshot name"
PARAM_DESC_COPIED_INSTANCE_TAGS = \
    "Enter a tag filter to copy tags from the RDS instance to the snapshot. For example, enter * to copy all tags from the " \
    "RDS instance to the snapshot."

PARAM_LABEL_STOPPED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_CREATE_SNAPSHOT = "Create snapshot"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_NAME = "Snapshot name"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied RDS instance tags"
PARAM_LABEL_RESTORE_PERMISSION = "Accounts with restore permissions"


class RdsStopInstanceAction(ActionBase):
    """
    Implements action to create image for an EC2 instance
    """
    properties = {
        ACTION_TITLE: "RDS Stop Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Stops RDS instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "07304978-7d2e-413b-bf50-893439891aba",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier," +
            "DBInstanceStatus:DBInstanceStatus,"
            "MultiAZ:MultiAZ," +
            "ReadReplicaSourceDBInstanceIdentifier:ReadReplicaSourceDBInstanceIdentifier," +
            "ReadReplicaDBInstanceIdentifiers:ReadReplicaDBInstanceIdentifiers," +
            "Engine:Engine,"
            "DBInstanceArn:DBInstanceArn}"
            "|[?contains(['available'],DBInstanceStatus)]",

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_STOPPED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STOPPED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STOPPED_INSTANCE_TAGS
            },
            PARAM_CREATE_SNAPSHOT: {
                PARAM_DESCRIPTION: PARAM_DESC_CREATE_SNAPSHOT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_CREATE_SNAPSHOT
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
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
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
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STOPPED_INSTANCE_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CREATE_SNAPSHOT,
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_RESTORE_PERMISSION
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:AddTagsToResource",
            "rds:RemoveTagsFromResource",
            "rds:DescribeDBInstances",
            "rds:ModifyDBsnapshotAttribute",
            "rds:DescribeDBSnapshots",
            "rds:ListTagsForResource",
            "rds:StopDBInstance",
            "tag:GetResources"
        ]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_instance = self._resources_

        self.db_instance_id = self.db_instance["DBInstanceIdentifier"]
        self.db_instance_arn = self.db_instance["DBInstanceArn"]
        self._rds_client = None

        self.create_snapshot = self.get(PARAM_CREATE_SNAPSHOT, True)

        # tags from the RDS instance
        self.instance_tags = self.db_instance.get("Tags", {})
        # filter for tags copied from RDS  instance to image
        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.accounts_with_restore_permissions = self.get(PARAM_RESTORE_PERMISSION, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "db-instance": self.db_instance_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        db_instance = arguments[ACTION_PARAM_RESOURCES]
        db_instance_id = db_instance["DBInstanceIdentifier"]
        account = db_instance["AwsAccount"]
        region = db_instance["Region"]
        return "{}-{}-{}-{}".format(account, region, db_instance_id, log_stream_date())

    @property
    def rds_client(self):

        if self._rds_client is None:
            self._rds_client = get_client_with_retries("rds",
                                                       methods=["describe_db_instances",
                                                                "add_tags_to_resource",
                                                                "remove_tags_from_resource",
                                                                "describe_db_snapshots",
                                                                "modify_db_snapshot_attribute",
                                                                "stop_db_instance"],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       context=self._context_,
                                                       logger=self._logger_)

        return self._rds_client

    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        db_id = resource["DBInstanceIdentifier"]

        if resource["Engine"] in ["aurora"]:
            logger.debug("For stopping Aurora clusters use RdsStopCluster action, instance {} skipped", db_id)
            return None

        if resource.get("ReadReplicaSourceDBInstanceIdentifier", None) is not None:
            logger.debug("Can not stop rds instance \"{}\" because it is a read replica of instance {}", db_id,
                         resource["ReadReplicaSourceDBInstanceIdentifier"])
            return None

        if len(resource.get("ReadReplicaDBInstanceIdentifiers", [])) > 0:
            logger.debug("Can not stop rds instance \"{}\" because it is the source for read copy instance(s) {}", db_id,
                         ",".join(resource["ReadReplicaDBInstanceIdentifiers"]))
            return None

        return resource

    def is_completed(self, exec_result):

        def set_tags_to_snapshot(last_snapshot, instance_tags):

            # tags on the snapshot
            tags = {}

            # tags copied from instance

            tags.update(self.copied_instance_tagfilter.pairs_matching_any_filter(instance_tags))

            tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_SOURCE_REGION: self._region_,
                                                  TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_,
                                                  TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id,

                                              }, restricted_value_set=True))

            try:
                tagging.set_rds_tags(rds_client=self.rds_client,
                                     resource_arns=[last_snapshot["DBSnapshotArn"]],
                                     tags=tags,
                                     can_delete=False,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_SNAPSHOT_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   last_snapshot["DBSnapshotArn"])
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_SNAPSHOT_TAGS, last_snapshot["DBSnapshotArn"], tag_ex)

        def set_instance_snapshot_tag(stopped_snapshot_name):
            tags = {SNAPSHOT_TAG.format(os.getenv(handlers.ENV_STACK_NAME)): stopped_snapshot_name}
            try:
                self.rds_client.add_tags_to_resource_with_retries(ResourceName=self.db_instance_arn, Tags=tag_key_value_list(tags))
                self._logger_.info(INF_SETTING_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   self.db_instance_arn)
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_INSTANCE_TAGS, self.db_instance_arn, tag_ex)

        def set_tags_to_stopped_instance(db_inst, snapshot_name=None):

            # tags on the started instance
            tags = self.build_tags_from_template(parameter_name=PARAM_STOPPED_INSTANCE_TAGS,
                                                 tag_variables={TAG_PLACEHOLDER_SNAPSHOT_ID:snapshot_name} if snapshot_name else {},
                                                 restricted_value_set=True)

            try:
                tagging.set_rds_tags(rds_client=self.rds_client,
                                     resource_arns=[db_inst["DBInstanceArn"]],
                                     tags=tags,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   db_inst["DBInstanceArn"])
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_INSTANCE_TAGS, db_inst["DBInstanceArn"], tag_ex)

        def grant_restore_permissions(snap_id):

            if self.accounts_with_restore_permissions is not None and len(self.accounts_with_restore_permissions) > 0:

                args = {
                    "DBSnapshotIdentifier": snap_id,
                    "AttributeName": "restore",
                    "ValuesToAdd": [a.strip() for a in self.accounts_with_restore_permissions]
                }

                try:
                    self.rds_client.modify_db_snapshot_attribute_with_retries(**args)
                    self._logger_.info(INF_GRANTING_RESTORE_PERMISSION, ", ".join(self.accounts_with_restore_permissions))
                    self.result["restore-access-accounts"] = [a.strip() for a in self.accounts_with_restore_permissions]
                except Exception as grant_ex:
                    raise_exception(ERR_SETTING_RESTORE_PERMISSION, self.accounts_with_restore_permissions, grant_ex)

        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        db_instance = rds.get(services.rds_service.DB_INSTANCES,
                              region=self._region_,
                              DBInstanceIdentifier=self.db_instance_id,
                              _expected_boto3_exceptions_=["DBInstanceNotFoundFault"])
        if db_instance is None:
            raise_exception(ERR_INSTANCE_NOT_FOUND, self.db_instance_id)

        if db_instance["DBInstanceStatus"] != INSTANCE_STATUS_STOPPED:
            return None

        snapshot_name = exec_result.get("snapshot-name", None)
        if snapshot_name is not None:
            snapshot = rds.get(services.rds_service.DB_SNAPSHOTS,
                               region=self.db_instance["Region"],
                               tags=False,
                               DBSnapshotIdentifier=snapshot_name,
                               _expected_boto3_exceptions_=["DBSnapshotNotFoundFault", "InvalidParameterValue"])

            if snapshot is None:
                return None

            # check status of snapshot
            snapshot_status = snapshot["Status"]

            # failed
            if snapshot_status == SNAPSHOT_STATUS_FAILED:
                raise_exception(ERR_SNAPSHOT_FAILED, snapshot_name, self.db_instance_id)

            # creating but not done yet
            if snapshot_status == SNAPSHOT_STATUS_CREATING:
                progress = snapshot.get("PercentProgress", 0)
                self._logger_.info(INF_CREATING_SNAPSHOT_PROGRESS, snapshot_name, self.db_instance_id, progress)
                return None

            # snapshot is available
            if snapshot_status == SNAPSHOT_STATUS_AVAILABLE:
                try:
                    set_tags_to_snapshot(snapshot, exec_result.get("instance-tags", {}))
                    set_instance_snapshot_tag(snapshot_name)
                    set_tags_to_stopped_instance(db_inst=db_instance, snapshot_name=snapshot_name)
                    grant_restore_permissions(snapshot_name)
                    return self.result
                except Exception as ex:
                    raise ex

            return None

        set_tags_to_stopped_instance(db_instance)

        return self.result

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_STOP_ACTION, self.db_instance_id, self._task_)

        args = {
            "DBInstanceIdentifier": self.db_instance_id
        }

        if self.create_snapshot:

            snapshot_name = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME,
                                                         tag_variables={
                                                             TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id
                                                         })
            if snapshot_name == "":
                dt = self._datetime_.utcnow()
                snapshot_name = SNAPSHOT_NAME.format(self.db_instance_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

            prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                                  tag_variables={
                                                      TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id
                                                  })

            snapshot_name = (prefix + snapshot_name).lower()

            args["DBSnapshotIdentifier"] = snapshot_name

            self.result["snapshot-name"] = snapshot_name
            self.result["instance-tags"] = self.db_instance.get("Tags", {})

        try:
            self.rds_client.stop_db_instance_with_retries(**args)
        except Exception as ex:
            raise_exception(ERR_STOP_INSTANCE, self.db_instance_id, ex)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StoppedDBInstances=1
        )

        return self.result
