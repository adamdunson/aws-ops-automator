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


import handlers.rds_tag_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_DB_INSTANCE_ID = "db-instance-id"
TAG_PLACEHOLDER_DB_SNAPSHOT_ID = "db-snapshot-id"

ERR_CREATING_SNAPSHOT = "Error creating snapshot for RDS instance {}, ex"
ERR_SETTING_INSTANCE_TAGS = "Error setting tags to RDS instance {}, {}"
ERR_ALREADY_IN_BACKUP = "Cannot create snapshot for RDS instance as the instance is already in backup-up state"
ERR_CREATING_DB_SNAPSHOT = "Failed to create snapshot for instance {}, snapshot id was {}"
ERR_GRANTING_PERMISSIONS = "Error granting restore permissions for created for accounts {}"
ERR_NOT_AVAILABLE = "Cannot create snapshot for RDS instance {} as the instance is not available"

WARN_NOT_AVAILABLE_FOR_BACKUP = "Cannot make snapshot from RDS instance {} as it is not available, it current status is {}"

GROUP_TITLE_TAGGING_NAMING = "Snapshot options"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

SNAPSHOT_STATE_FAILED = "failed"
SNAPSHOT_STATE_AVAILABLE = "available"
SNAPSHOT_STATE_CREATING = "creating"

INFO_START_SNAPSHOT_ACTION = "Creating snapshot for RDS instance {} for task {}"
INF_SET_INSTANCE_TAGS = "Set tags {} to RDS instance {}"
INF_COMPLETED = "Creation of snapshot completed"
INF_NO_SNAPSHOT_YET = "Snapshot {} not created yet"
INF_PROGRESS = "Snapshot {} is creating, progress is {}%"
INF_SNAPSHOT_STARTED = "Creation of snapshot {} for RDS instance {} started"
INF_START_CHECK = "Checking completion of RDS snapshot with starting information"

PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_RESTORE_PERMISSION = "GrantRestorePermission"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_SNAPSHOT_NAME = "SnapshotName"

PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to the created snapshots. Note that tag values for RDS cannot contain ',' characters.  When specifying multiple " \
    "follow up tasks in the value of the Ops Automator task list tag use  a '/' character instead"
PARAM_DESC_SNAPSHOT_NAME = "Name of the created snapshot, leave blank for default snapshot name"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for name of created snapshots."
PARAM_DESC_INSTANCE_TAGS = "Tags to set on source RDS instance after the snapshot has been created successfully. " \
                           "When triggering this task using tagging events, make sure the new tags do not re-trigger this task."
PARAM_DESC_RESTORE_PERMISSION = "Accounts authorized to copy or restore the RDS snapshot"
PARAM_DESC_COPIED_INSTANCE_TAGS = \
    "Enter a tag filter to copy tags from the RDS instance to the snapshot. For example, enter * to copy all tags from the " \
    "RDS instance to the snapshot."

PARAM_LABEL_SNAPSHOT_NAME = "Snapshot name"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied RDS instance tags"
PARAM_LABEL_RESTORE_PERMISSION = "Accounts with restore permissions"
PARAM_LABEL_INSTANCE_TAGS = "RDS Instance tags"


class RdsCreateInstanceSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Create Instance Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Creates snapshot for RDS Instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "69eaa1af-445d-4bfa-a5c0-cb3ff654c0de",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_SELECT_EXPRESSION: "DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier,Tags:Tags,"
                                  "DBInstanceStatus:DBInstanceStatus, DBInstanceArn:DBInstanceArn}",

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

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
            PARAM_RESTORE_PERMISSION: {
                PARAM_DESCRIPTION: PARAM_DESC_RESTORE_PERMISSION,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_RESTORE_PERMISSION
            },
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_INSTANCE_TAGS,
                    PARAM_RESTORE_PERMISSION
                ],
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:CreateDBSnapshot",
            "rds:AddTagsToResource",
            "rds:RemoveTagsFromResource",
            "rds:DescribeDBSnapshots",
            "rds:ListTagsForResource",
            "rds:ModifyDBSnapshotAttribute",
            "tag:GetResources"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_instance = self._resources_

        self.db_instance_id = self.db_instance["DBInstanceIdentifier"]
        self.db_instance_arn = self.db_instance["DBInstanceArn"]
        self._rds_client = None

        self.accounts_with_restore_permissions = self.get(PARAM_RESTORE_PERMISSION, [])

        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "db-instance": self.db_instance_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        db_instance = arguments[ACTION_PARAM_RESOURCES]
        account = db_instance["AwsAccount"]
        db_instance_id = db_instance["DBInstanceIdentifier"]
        region = db_instance["Region"]
        return "{}-{}-{}-{}".format(account, region, db_instance_id, log_stream_date())

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):
        if resource["DBInstanceStatus"] != "available":
            logger.warning(WARN_NOT_AVAILABLE_FOR_BACKUP, resource["DBInstanceIdentifier"], resource["DBInstanceStatus"])
            return None

        return resource

    @property
    def rds_client(self):
        if self._rds_client is None:
            methods = [
                "create_db_snapshot",
                "describe_db_snapshots",
                "add_tags_to_resource",
                "remove_tags_from_resource",
                "modify_db_snapshot_attribute"
            ]
            self._rds_client = get_client_with_retries("rds",
                                                       methods=methods,
                                                       region=self.db_instance["Region"],
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._rds_client

    def is_completed(self, snapshot_create_data):

        def grant_restore_permissions(snapshot_id):

            if self.accounts_with_restore_permissions is not None and len(self.accounts_with_restore_permissions) > 0:

                args = {
                    "DBSnapshotIdentifier": snapshot_id,
                    "AttributeName": "restore",
                    "ValuesToAdd": [a.strip() for a in self.accounts_with_restore_permissions]
                }

                try:
                    self.rds_client.modify_db_snapshot_attribute_with_retries(**args)
                    self._logger_.info("Granting restore permissions to accounts {}",
                                       ", ".join(self.accounts_with_restore_permissions))
                    self.result["restore-access-accounts"] = [a.strip() for a in self.accounts_with_restore_permissions]
                except Exception as ex:
                    raise_exception(ERR_GRANTING_PERMISSIONS, self.accounts_with_restore_permissions, ex)

        def add_tags_to_rds_instance(snapshot_id):
            tags = self.build_tags_from_template(parameter_name=PARAM_INSTANCE_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_DB_SNAPSHOT_ID: snapshot_id
                                                 }, restricted_value_set=True)

            if len(tags) > 0:
                try:
                    tagging.set_rds_tags(rds_client=self.rds_client,
                                         resource_arns=[self.db_instance_arn],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INF_SET_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                       self.db_instance_id)
                except Exception as ex:
                    raise_exception(ERR_SETTING_INSTANCE_TAGS, self.db_instance_id, ex)

        self._logger_.info(INF_START_CHECK, safe_json(snapshot_create_data, indent=3))

        db_snapshot_id = snapshot_create_data["db-snapshot-id"]
        self._logger_.info("RDS snapshot is is {}", db_snapshot_id)

        # create service instance to test is snapshots are available
        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        db_snapshot = rds.get(services.rds_service.DB_SNAPSHOTS,
                              region=self.db_instance["Region"],
                              tags=True,
                              DBSnapshotIdentifier=db_snapshot_id)

        if db_snapshot is None:
            self._logger_.info(INF_NO_SNAPSHOT_YET, db_snapshot_id)
            return None

        self._logger_.debug("Snapshot data", safe_json(db_snapshot))

        # get status
        status = db_snapshot['Status']

        if status == "failed":
            raise Exception(
                ERR_CREATING_DB_SNAPSHOT.format(self.db_instance, db_snapshot_id))

        if status == "creating":
            progress = db_snapshot.get("PercentProgress", 0)
            self._logger_.info(INF_PROGRESS, db_snapshot_id, progress)
            return None

        if status == "available":
            add_tags_to_rds_instance(db_snapshot_id)
            grant_restore_permissions(db_snapshot_id)
            self.result["size"] = db_snapshot['AllocatedStorage']

        self._logger_.info(INF_COMPLETED)
        return self.result

    def execute(self):

        def create_rds_snapshot():
            rds = self.rds_client

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

            snapshot_name = prefix + snapshot_name

            # set snapshot tags from param, source tags copy is part of rds instance so need to set these
            tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.db_instance.get("Tags", {}))
            tags.update(
                self.build_tags_from_template(PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id
                                              },
                                              restricted_value_set=True))

            tags[MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID.format(os.getenv(handlers.ENV_STACK_NAME))] = self.db_instance_id

            try:
                response = rds.create_db_snapshot_with_retries(DBInstanceIdentifier=self.db_instance_id,
                                                               DBSnapshotIdentifier=snapshot_name,
                                                               Tags=tag_key_value_list(tags))

                return response["DBSnapshot"]["DBSnapshotIdentifier"]

            except Exception as ex:
                raise_exception(ERR_CREATING_SNAPSHOT, self.db_instance_id, ex)

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INFO_START_SNAPSHOT_ACTION, self.db_instance_id, self._task_)

        if self.db_instance["DBInstanceStatus"] == "backing-up":
            raise_exception(ERR_ALREADY_IN_BACKUP, self.db_instance_id)

        if self.db_instance["DBInstanceStatus"] != "available":
            raise_exception(ERR_NOT_AVAILABLE, self.db_instance_id)

        snapshot_id = create_rds_snapshot()

        self.result["db-snapshot-id"] = snapshot_id

        self._logger_.info(INF_SNAPSHOT_STARTED, snapshot_id, self.db_instance_id)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CreatedDBSnapshots=1)

        return self.result
