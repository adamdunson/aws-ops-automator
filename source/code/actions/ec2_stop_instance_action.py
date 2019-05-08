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


import time

from botocore.exceptions import ClientError

import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception

EC2_STATE_SHUTTING_DOWN = 32
EC2_STATE_STOPPED = 80
EC2_STATE_STOPPING = 64
EC2_STATE_TERMINATED = 48

EC2_STOPPING_STATES = [EC2_STATE_SHUTTING_DOWN, EC2_STATE_STOPPING, EC2_STATE_STOPPED, EC2_STATE_TERMINATED]

PARAM_STOPPED_INSTANCE_TAGS = "StoppedInstanceTags"
PARAM_HIBERNATION = "Hibernate"

PARAM_DESC_STOPPED_INSTANCE_TAGS = "Tags to set on stopped EC2 instance."
PARAM_DESC_HIBERNATION = "Hibernate stopped instances that meet hibernation prerequisites. If these prerequisites are not met, " \
                         "then the instances will be stopped without hibernation."

PARAM_LABEL_STOPPED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_HIBERNATION = "Hibernate stopped instances"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options"

ERR_SETTING_INSTANCE_TAGS = "Error setting tags to stopped instance {}, {}"
ERR_STOPPING = "Error stopping instances {}, {}"
ERR_INSTANCE_NOT_IN_STOPPING_STATE = "Instance {} is not in a stopping state, state is {}"
ERR_SET_TAGS = "Can not set tags to stopped instance {}, {}"

INF_INSTANCE_STOP_ACTION = "Stopping EC2 instance {} for task {}"
INF_SET_INSTANCE_TAGS = "Set tags {} to stopped instance {}"
INF_NOT_STOPPED_YET = "Instance {} is not stopped yet, current status is {}"

WARN_NO_HIBERNATION = "Cannot hibernate instance {} ({}), trying to stop without hibernation"


class Ec2StopInstanceAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Stop Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Stops EC2 instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "7f7d36cb-1428-4060-922c-4fe442de9798",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_BATCH_SIZE: 20,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[].{State:State.Name,InstanceId:InstanceId, Tags:Tags}" +
            "|[?contains(['running','pending'],State)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_HIBERNATION: {
                PARAM_DESCRIPTION: PARAM_DESC_HIBERNATION,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_HIBERNATION
            },

            PARAM_STOPPED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STOPPED_INSTANCE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STOPPED_INSTANCE_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_HIBERNATION,
                    PARAM_STOPPED_INSTANCE_TAGS
                ],
            },
        ],

        ACTION_PERMISSIONS: [
            "ec2:StopInstances",
            "ec2:DescribeTags",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_

        self.hibernation = self.get(PARAM_HIBERNATION, False)

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instances": self.instance_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        instance = arguments[ACTION_PARAM_RESOURCES]
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}-{}".format(account, region, instance_id, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "stop_instances",
                "create_tags",
                "delete_tags"
            ]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    def _get_stopped_instance(self):
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        return ec2.get(services.ec2_service.INSTANCES,
                       InstanceIds=[self.instance_id],
                       region=self._region_,
                       select="Reservations[*].Instances[].{StateName:State.Name,State:State.Code,InstanceId:InstanceId}")

    # noinspection PyUnusedLocal
    def is_completed(self, stop_action_data):

        instance = self._get_stopped_instance()

        self._logger_.info("Instance {}", safe_json(instance, indent=3))

        done = (instance["State"] & 0xFF) in [EC2_STATE_STOPPED, EC2_STATE_TERMINATED]

        if not done:
            self._logger_.info(INF_NOT_STOPPED_YET, self.instance_id, instance["StateName"])
            return None

        # set tags
        tags = self.build_tags_from_template(parameter_name=PARAM_STOPPED_INSTANCE_TAGS)
        try:
            if len(tags) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=[self.instance_id],
                                     tags=tags,
                                     logger=self._logger_)
        except Exception as ex:
            self._logger_.error(ERR_SET_TAGS, ','.join(self.instance_id), str(ex))

        return self.result

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_STOP_ACTION, self.instance_id, self._task_)

        def is_in_stopping_state(state):
            return (state & 0xFF) in EC2_STOPPING_STATES

        try:
            args = {
                "InstanceIds": [self.instance_id],
                "Hibernate": self.hibernation
            }

            try:
                self.ec2_client.stop_instances_with_retries(**args)
            except ClientError as ex:
                if ex.response.get("Error", {}).get("Code") == "UnsupportedHibernationConfiguration" or \
                        ex.response.get("Error", {}).get("Code") == "UnsupportedOperation":
                    self._logger_.warning(WARN_NO_HIBERNATION, self.instance_id, ex)
                    args["Hibernate"] = False
                    self.ec2_client.stop_instances_with_retries(**args)
                else:
                    raise ex

            time.sleep(15)
            stopped_instance = self._get_stopped_instance()

            if not is_in_stopping_state(stopped_instance["State"]):
                self._logger_.error(ERR_INSTANCE_NOT_IN_STOPPING_STATE, self.instance_id, stopped_instance["StateName"])
        except Exception as ex:
            raise_exception(ERR_STOPPING, ",".join(self.instance_id), str(ex))

        self.result["stopped-instance"] = self.instance_id

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StoppedInstances=1
        )

        return self.result
