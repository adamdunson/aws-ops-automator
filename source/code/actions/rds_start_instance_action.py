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
from outputs import raise_exception

SNAPSHOT_TAG = "ops-automator-{}-rds-stop-snapshot"

ERR_SNAPSHOT_DELETE_FAILED = "Error deleting snapshot {} for instance {}, {}"
ERR_START_INSTANCE = "Error starting instance {}, {}"
ERR_REMOVING_INSTANCE_TAGS = "Error removing tags from instance {}, {}"
ERR_SETTING_INSTANCE_TAGS = "Error setting tags to instance {}, {}"
ERR_INSTANCE_NOT_FOUND = "Started instance {} does not longer exist"

INF_INSTANCE_START_ACTION = "Starting RDS instance {} for task {}"
INF_SETTING_INSTANCE_TAGS = "Setting tags {} to instance {}"

INSTANCE_STATUS_AVAILABLE = "available"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options"
GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot options"

PARAM_STARTED_INSTANCE_TAGS = "StartedInstanceTags"
PARAM_DELETE_SNAPSHOT = "DeleteSnapshot"

PARAM_DESC_STARTED_INSTANCE_TAGS = \
    "Tags to set on started RDS instance. Note that tag values for RDS cannot contain ',' characters. When specifying multiple " \
    "follow up tasks in the value of the Ops Automator task list tag use a '/' character instead"
PARAM_DESC_DELETE_SNAPSHOT = "Deletes the snapshot that was snapshot created before stopping the RDS instance."

PARAM_LABEL_STARTED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_DELETE_SNAPSHOT = "Delete snapshot"


class RdsStartInstanceAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Start Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Starts RDS instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "a8e57654-75a7-4020-a045-797d6f54ce2f",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_ALLOW_TAGFILTER_WILDCARD: True,

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_SELECT_EXPRESSION:
            "DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier," +
            "DBInstanceStatus:DBInstanceStatus,"
            "MultiAZ:MultiAZ," +
            "ReadReplicaSourceDBInstanceIdentifier:ReadReplicaSourceDBInstanceIdentifier," +
            "ReadReplicaDBInstanceIdentifiers:ReadReplicaDBInstanceIdentifiers," +
            "Engine:Engine,"
            "DBInstanceArn:DBInstanceArn}"
            "|[?contains(['stopped'],DBInstanceStatus)]",

        ACTION_PARAMETERS: {

            PARAM_STARTED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STARTED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STARTED_INSTANCE_TAGS
            },
            PARAM_DELETE_SNAPSHOT: {
                PARAM_DESCRIPTION: PARAM_DESC_DELETE_SNAPSHOT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_DELETE_SNAPSHOT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STARTED_INSTANCE_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_DELETE_SNAPSHOT
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:AddTagsToResource",
            "rds:DescribeDBInstances",
            "rds:DeleteDBSnapshot",
            "rds:DescribeDBSnapshots",
            "rds:RemoveTagsFromResource",
            "rds:ListTagsForResource",
            "rds:StartDBInstance",
            "tag:GetResources"
        ]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_instance = self._resources_

        self.db_instance_id = self.db_instance["DBInstanceIdentifier"]
        self.db_instance_arn = self.db_instance["DBInstanceArn"]
        self._rds_client = None

        # delete snapshot created before stopping the instance
        self.delete_snapshot = self.get(PARAM_DELETE_SNAPSHOT, False)

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
                                                       methods=[
                                                           "describe_db_instances",
                                                           "add_tags_to_resource",
                                                           "describe_db_snapshots",
                                                           "delete_db_snapshot",
                                                           "remove_tags_from_resource",
                                                           "start_db_instance"],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       context=self._context_)

        return self._rds_client

    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        db_id = resource["DBInstanceIdentifier"]

        if resource["Engine"] in ["aurora"]:
            logger.debug("For starting RDS Aurora clusters use RdsStartCluster action, instance {} skipped", db_id)
            return None

        if resource.get("ReadReplicaSourceDBInstanceIdentifier", None) is not None:
            logger.debug("Can not start rds instance \"{}\" because it is a read replica of instance {}", db_id,
                         resource["ReadReplicaSourceDBInstanceIdentifier"])
            return None

        if len(resource.get("ReadReplicaDBInstanceIdentifiers", [])) > 0:
            logger.debug("Can not start rds instance \"{}\" because it is the source for read copy instance(s) {}", db_id,
                         ",".join(resource["ReadReplicaDBInstanceIdentifiers"]))
            return None

        return resource

    def is_completed(self, _):

        def set_tags_to_started_instance(db_inst):

            # tags on the started instance
            tags = self.build_tags_from_template(parameter_name=PARAM_STARTED_INSTANCE_TAGS, restricted_value_set=True)

            try:
                tagging.set_rds_tags(rds_client=self.rds_client,
                                     resource_arns=[db_inst["DBInstanceArn"]],
                                     tags=tags,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   db_inst["DBInstanceArn"])
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_INSTANCE_TAGS, db_inst["DBInstanceArn"], tag_ex)

        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        db_instance = rds.get(services.rds_service.DB_INSTANCES,
                              region=self._region_,
                              DBInstanceIdentifier=self.db_instance_id,
                              tags=True,
                              _expected_boto3_exceptions_=["DBInstanceNotFoundFault"])

        if db_instance is None:
            raise_exception(ERR_INSTANCE_NOT_FOUND, self.db_instance_id)

        if db_instance["DBInstanceStatus"] != INSTANCE_STATUS_AVAILABLE:
            return None

        set_tags_to_started_instance(db_instance)

        if self.delete_snapshot:
            snapshot_tag_name = SNAPSHOT_TAG.format(os.getenv(handlers.ENV_STACK_NAME))
            snapshot_name = db_instance.get("Tags", {}).get(snapshot_tag_name, None)

            if snapshot_name is not None:
                snapshot = rds.get(services.rds_service.DB_SNAPSHOTS,
                                   region=self.db_instance["Region"],
                                   tags=False,
                                   DBSnapshotIdentifier=snapshot_name,
                                   _expected_boto3_exceptions_=["DBSnapshotNotFoundFault"])

                if snapshot is not None:
                    try:
                        self.rds_client.delete_db_snapshot_with_retries(DBSnapshotIdentifier=snapshot_name)
                    except Exception as ex:
                        self._logger_.error(ERR_SNAPSHOT_DELETE_FAILED, snapshot_name, self.db_instance, ex)

                try:
                    self.rds_client.remove_tags_from_resource_with_retries(ResourceName=self.db_instance_arn,
                                                                           TagKeys=[snapshot_tag_name])
                except Exception as ex:
                    self._logger_.error(ERR_REMOVING_INSTANCE_TAGS, self.db_instance_id, ex)

        return self.result

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_START_ACTION, self.db_instance_id, self._task_)

        try:
            self.rds_client.start_db_instance_with_retries(DBInstanceIdentifier=self.db_instance_id)
        except Exception as ex:
            raise_exception(ERR_START_INSTANCE, self.db_instance_id, ex)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StartedDBInstances=1
        )

        return self.result
