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

import handlers.ec2_tag_event_handler
import services.ec2_service
from actions import *
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries
from outputs import raise_exception

PARAM_REBOOTED_INSTANCE_TAGS = "RebootedInstanceTags"
PARAM_DESC_REBOOTED_INSTANCE_TAGS = "Tags to set on rebooted EC2 instance. Don't use tag updates with a " \
                                    "tag filter that could re-trigger the execution of this task."
PARAM_LABEL_REBOOTED_INSTANCE_TAGS = "Instance tags"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance reboot options"

ERR_SETTING_INSTANCE_TAGS = "Error setting tags to rebooted instance {}, {}"
ERR_REBOOTING = "Error rebooting instances {}, {}"

INF_INSTANCE_REBOOT_ACTION = "Rebooting EC2 instances {} for task {}"
INF_SET_INSTANCE_TAGS = "Set tags {} to rebooted instance {}"


class Ec2RebootInstanceAction(ActionEc2EventBase):
    properties = {
        ACTION_TITLE: "EC2 Reboot Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Reboots EC2 instances",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "57823a7b-4e1c-4933-a03e-e00314e60359",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_BATCH_SIZE: 25,

        ACTION_MIN_INTERVAL_MIN: 5,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[].{State:State.Name,InstanceId:InstanceId, Tags:Tags}" +
            "|[?contains(['running'],State)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_REBOOTED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_REBOOTED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REBOOTED_INSTANCE_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_REBOOTED_INSTANCE_TAGS
                ],
            },
        ],

        ACTION_PERMISSIONS: [
            "ec2:RebootInstances",
            "ec2:DescribeTags",
            "ec2:DescribeInstances",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionEc2EventBase.__init__(self, action_arguments, action_parameters)

        self.instances = self._resources_

        self.instance_ids = [i["InstanceId"] for i in self.instances]
        self._ec2_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instances": self.instance_ids,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]

        if len(arguments.get(ACTION_PARAM_EVENT, {})) > 0:
            instances = list(set([s["InstanceId"] for s in arguments.get(ACTION_PARAM_RESOURCES, [])]))
            if len(instances) == 1:
                return "{}-{}-{}-{}".format(account, region, instances[0], log_stream_date())
            else:
                return "{}-{}-{}-{}".format(account, region, arguments[ACTION_ID], log_stream_date())
        else:
            return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = ["reboot_instances", "create_tags", "delete_tags", "describe_instances"]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        ActionEc2EventBase.check_tag_filters_and_tags(parameters, task_settings, [PARAM_REBOOTED_INSTANCE_TAGS], logger)

        return parameters

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_REBOOT_ACTION, ','.join(self.instance_ids), self._task_)

        try:
            self.ec2_client.reboot_instances_with_retries(InstanceIds=self.instance_ids)

        except Exception as ex:
            raise_exception(ERR_REBOOTING, ",".join(self.instance_ids), str(ex))

        tags = self.build_tags_from_template(parameter_name=PARAM_REBOOTED_INSTANCE_TAGS)
        self.set_ec2_instance_tags_with_event_loop_check(client=self.ec2_client, instance_ids=self.instance_ids, tags_to_set=tags)

        self.result["rebooted-instances"] = self.instance_ids

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            RebootedInstances=len(self.instance_ids)
        )

        return self.result
