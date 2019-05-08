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
from actions import *
from actions.action_rds_events_base import ActionRdsEventBase
from boto_retry import get_client_with_retries
from outputs import raise_exception

PARAM_REBOOTED_INSTANCE_TAGS = "RebootedInstanceTags"
PARAM_FORCE_FAILOVER = "ForceFailover"

PARAM_DESC_REBOOTED_INSTANCE_TAGS = "Tags to set on rebooted EC2 instance. Don't use tag updates with a " \
                                    "tag filter that could trigger a new execution of this task."
PARAM_DESC_FORCE_FAILOVER = "When set,the reboot is conducted through a MultiAZ failover. " \
                            "This option is ignored if the instance is not configured for MultiAZ."

PARAM_LABEL_REBOOTED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_FORCE_FAILOVER = "Force failover"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance reboot options"

ERR_SETTING_INSTANCE_TAGS = "Error setting tags to rebooted instance {}, {}"
ERR_REBOOTING = "Error rebooting instance {}, {}"

INF_INSTANCE_REBOOT_ACTION = "Rebooting RDS instance {} for task {}"
INF_SET_INSTANCE_TAGS = "Set tags {} to rebooted instance {}"


class RdsRebootInstanceAction(ActionRdsEventBase):
    properties = {
        ACTION_TITLE: "RDS Reboot Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Reboots RDS instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "22448fb2-8a69-475a-9524-f96ff8f5cb97",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier," +
            "DBInstanceStatus:DBInstanceStatus,"
            "MultiAZ:MultiAZ," +
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

            PARAM_REBOOTED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_REBOOTED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REBOOTED_INSTANCE_TAGS
            },
            PARAM_FORCE_FAILOVER: {
                PARAM_DESCRIPTION: PARAM_DESC_FORCE_FAILOVER,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_FORCE_FAILOVER

            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_FORCE_FAILOVER,
                    PARAM_REBOOTED_INSTANCE_TAGS,
                ],
            },
        ],

        ACTION_PERMISSIONS: [
            "rds:RebootDBInstance",
            "rds:AddTagsToResource",
            "rds:DescribeDBInstances",
            "rds:ListTagsForResource",
            "rds:RemoveTagsFromResource",
            "tag:GetResources"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionRdsEventBase.__init__(self, action_arguments, action_parameters)

        self.db_instance = self._resources_

        self.db_instance_id = self.db_instance["DBInstanceIdentifier"]
        self.db_instance_arn = self.db_instance["DBInstanceArn"]
        self._rds_client = None

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
            methods = [
                "reboot_db_instance",
                "add_tags_to_resource",
                "remove_tags_from_resource",
                "describe_db_instances"
            ]

            self._rds_client = get_client_with_retries("rds",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._rds_client

    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        ActionRdsEventBase.check_tag_filters_and_tags(parameters, task_settings, [PARAM_REBOOTED_INSTANCE_TAGS], logger)

        return parameters

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_REBOOT_ACTION, self.db_instance_id, self._task_)

        try:
            force_failover = self.get(PARAM_FORCE_FAILOVER, False) and self.db_instance.get("MultiAZ", False)
            self.rds_client.reboot_db_instance_with_retries(DBInstanceIdentifier=self.db_instance_id, ForceFailover=force_failover)

        except Exception as ex:
            raise_exception(ERR_REBOOTING, self.db_instance_id, str(ex))

        tags = self.build_tags_from_template(parameter_name=PARAM_REBOOTED_INSTANCE_TAGS)
        self.set_rds_instance_tags_with_event_loop_check(db_instance_id=self.db_instance_id,
                                                         tags_to_set=tags,
                                                         client=self.rds_client,
                                                         region=self._region_)

        self.result["rebooted-db-instance"] = self.db_instance_id

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            RebootedInstances=1)

        return self.result
