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


import re as regex

from botocore.exceptions import ClientError

import handlers.ec2_state_event_handler
import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from helpers.timer import Timer
from outputs import raise_value_error
from services.ec2_service import Ec2Service
from tagging import tag_key_value_list

INSUFFICIENT_CAPACITY = "InsufficientInstanceCapacity"
ERR_INVALID_ALTERNATIVE_ENTRY = "Entry {} is not a valid entry for specifying alternative instance types"

TAG_ORIGINAL_INST_SIZE = "ops-automator-start-instance:original-instance-size"

INSTANCE_TYPE_PATTERN = r"\w\d\.(((2|4|8|16|32)?xlarge)|nano|micro|small|medium|large)"
INSTANCE_ALTERNATIVES_ENTRY_PATTERN = r"^{0}={0}(\|{0})*$".format(INSTANCE_TYPE_PATTERN)

EC2_STATE_PENDING = 0
EC2_STATE_RUNNING = 16
EC2_STATE_STOPPED = 80

EC2_STARTING_STATES = {EC2_STATE_PENDING, EC2_STATE_RUNNING}

PARAM_STARTED_INSTANCE_TAGS = "StartedInstanceTags"
PARAM_ALT_INSTANCE_SIZES = "AlternativeInstanceTypes"
PARAM_TEST_UNAVAILABLE_TYPES = "NotAvailableTypes"

PARAM_DESC_STARTED_INSTANCE_TAGS = "Tags to set on started EC2 instance."
PARAM_DESC_ALT_INSTANCE_TYPES = "Alternative sizes for started instance when no capacity is available. This parameter is a " \
                                "comma separated list with entries in the format size=alternatives, where " \
                                "size is the original instance size and " \
                                "alternatives is a \"|\" separated list of valid and compatible instance sizes. " \
                                "For example t2.micro=t2.small|t2.medium,c4.large=c4.xlarge"

PARAM_LABEL_STOPPED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_ALT_INSTANCE_TYPES = "Alternative instance sizes"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options (For starting instances with encrypted volumes make sure to grant " \
                               "'kms:CreateGrant' permission for the used kms key to the Ops Automator role)"

ERR_INSTANCE_NOT_IN_STARTING_STATE = "Instance {} is not in a starting state, state is {}"
ERR_INVALID_ENTRY = "Entry {} in {} parameter is not a valid entry, {} is not a valid instant size"
ERR_RESTORING_ORIGINAL_TYPE = "Error restoring original instance size of instance {} to  {}, {}"
ERR_SETTING_ALT_INSTANCE_TYPE = "Error setting alternative instance size for instance {} to  {}, {}"
ERR_STARTING = "Error starting instance {}, {}"
ERR_SET_TAGS = "Can not set tags to started instance {}, {}"

INF_ALT_TYPES = "Alternative sizes are {}"
INF_INSTANCE_DATA_NOT_STARTING = "Instance {} not in a starting state, instance data is {}"
INF_INSTANCE_RUNNING = "Instance {} is running"
INF_INSTANCE_STOP_ACTION = "Starting EC2 instance {} for task {}"
INF_NO_CAPACITY_WILL_RETRY = "No capacity for instance size {}, retries will be done"
INF_NO_TYPE_CAPACITY = "Not enough capacity for size {}"
INF_RETRY_START = "Retry to start instance {}"
INF_SET_ALT_TYPE = "Setting instance size for instance {} to alternative size {}"

ERR_INSTANCE_RESIZING = "Error resizing started  instance {} to size {}, {}"

WARN_NO_TYPE_CAPACITY = "Not enough capacity for size {}"


class Ec2StartInstanceAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Start Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Starts EC2 instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "a12cbabe-8be3-4787-9d8a-348dfdbf4edf",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[].{State:State.Name,InstanceId:InstanceId, InstanceType:InstanceType, Tags:Tags}" +
            "|[?contains(['stopped'],State)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT],
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: [
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_STARTED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STARTED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STOPPED_INSTANCE_TAGS
            },
            PARAM_ALT_INSTANCE_SIZES: {
                PARAM_DESCRIPTION: PARAM_DESC_ALT_INSTANCE_TYPES,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ALT_INSTANCE_TYPES
            }
            ,
            PARAM_TEST_UNAVAILABLE_TYPES: {
                # This is a hidden test parameter and is used to simulate situations where instance sizes are not available
                PARAM_DESCRIPTION: "",
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: "",
                PARAM_HIDDEN: True
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STARTED_INSTANCE_TAGS,
                    PARAM_ALT_INSTANCE_SIZES

                ],
            },
        ],

        ACTION_PERMISSIONS: [
            "ec2:StartInstances",
            "ec2:DescribeTags",
            "ec2:ModifyInstanceAttribute",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self._instance_ = self._resources_

        self.instance_id = self._instance_["InstanceId"]
        self._ec2_client = None

        self._resizing_data = None
        self.instance_size = None

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

    def _get_instance(self):
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        return ec2.get(services.ec2_service.INSTANCES,
                       InstanceIds=[self.instance_id],
                       region=self._region_,
                       select="Reservations[*].Instances[].{"
                              "Tags:Tags,"
                              "StateName:State.Name,"
                              "StateCode:State.Code,"
                              "StateStateReasonMessage:StateReason.Message,"
                              "InstanceType:InstanceType,"
                              "InstanceId:InstanceId}")

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        alternative_sizes = parameters.get(PARAM_ALT_INSTANCE_SIZES, "").strip()
        if alternative_sizes == "":
            return parameters

        valid_types = Ec2Service.valid_instance_types()
        for entry in [e.strip() for e in alternative_sizes.split(",")]:
            if regex.match(INSTANCE_ALTERNATIVES_ENTRY_PATTERN, entry) is None:
                raise_value_error(ERR_INVALID_ENTRY, entry, PARAM_ALT_INSTANCE_SIZES)
            size = entry.split("=")[0].strip()

            if len(valid_types) > 0:
                if size not in valid_types:
                    raise_value_error(ERR_INVALID_ENTRY, entry, PARAM_ALT_INSTANCE_SIZES, size)
                for size in (entry.split("=")[1]).split("|"):
                    if size.strip() not in valid_types:
                        raise_value_error(ERR_INVALID_ENTRY, format(entry, PARAM_ALT_INSTANCE_SIZES), size)
        return parameters

    @property
    def resizing_data(self):
        if self._resizing_data is None:
            self._resizing_data = {}
            alt_param_value = self.get(PARAM_ALT_INSTANCE_SIZES, "").strip()
            if alt_param_value != "":
                for entry in [e.strip() for e in alt_param_value.split(",")]:
                    if regex.match(INSTANCE_ALTERNATIVES_ENTRY_PATTERN, entry) is not None:
                        tmp = entry.partition("=")
                        self._resizing_data[tmp[0]] = [s.strip() for s in tmp[2].split("|")]
                    else:
                        self._logger_.warning(ERR_INVALID_ALTERNATIVE_ENTRY, entry)
                        raise_value_error(ERR_INVALID_ALTERNATIVE_ENTRY, entry)
        return self._resizing_data

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "start_instances",
                "create_tags",
                "delete_tags",
                "modify_instance_attribute"
            ]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    # noinspection PyUnusedLocal
    def is_completed(self, start_results):

        # get current state of instance
        instance = self._get_instance()
        self._logger_.info("Instance status data is {}", safe_json(instance, indent=3))

        state_code = instance["StateCode"] & 0xFF

        if state_code == EC2_STATE_RUNNING:

            # instance is running
            self._logger_.info(INF_INSTANCE_RUNNING, self.instance_id)

            # set tags
            tags = self.build_tags_from_template(parameter_name=PARAM_STARTED_INSTANCE_TAGS)
            try:
                if len(tags) > 0:
                    tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                         resource_ids=[self.instance_id],
                                         tags=tags,
                                         logger=self._logger_)
            except Exception as ex:
                self._logger_.error(ERR_SET_TAGS, ','.join(self.instance_id), str(ex))

            return self.result

        # in pending state, wait for next completion check
        if state_code == EC2_STATE_PENDING:
            return None

        if state_code == EC2_STATE_STOPPED:
            raise Exception(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, instance)

        return None

    @classmethod
    def is_in_starting_state(cls, state):
        return (state & 0xFF) in EC2_STARTING_STATES if state is not None else False

    @classmethod
    def insufficient_capacity(cls, ex):
        return type(ex).__name__ == "ClientError" and ex.response.get("Error", {}).get("Code", None) == INSUFFICIENT_CAPACITY

    def _start_instance(self):

        def is_in_starting_or_running_state(state):
            return (state & 0xFF) in EC2_STARTING_STATES if state is not None else False

        self.ec2_client.start_instances_with_retries(InstanceIds=[self.instance_id])

        with Timer(timeout_seconds=60, start=True) as t:
            started_instance = self._get_instance()

            # get state of started instance
            current_state = started_instance["StateCode"]

            if is_in_starting_or_running_state(current_state):
                # instance is starting
                return
            else:
                if t.timeout:
                    self._logger_.info(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, current_state)
                    raise Exception(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, current_state)

    def _resize_instance(self):
        if self._get_instance()["InstanceType"] != self.instance_size:
            self._logger_.info("Setting instance size of instance {} to {}", self.instance_id, self.instance_size)
            try:
                self.ec2_client.modify_instance_attribute_with_retries(InstanceId=self.instance_id,
                                                                       InstanceType={"Value": self.instance_size})
            except Exception as ex:
                self._logger_.error(ERR_INSTANCE_RESIZING, self.instance_id, self.instance_size, ex)

    def _test_simulate_insufficient_instance_capacity(self):

        if self.instance_size in self.get(PARAM_TEST_UNAVAILABLE_TYPES, []):
            raise ClientError(
                {
                    "Error": {
                        "Code": INSUFFICIENT_CAPACITY,
                        "Message": "Simulated {} Exception".format(INSUFFICIENT_CAPACITY)
                    }
                }, operation_name="start_instances")

    def execute(self):

        def set_original_instance_size():
            self.instance_size = self._get_instance()["InstanceType"]

            # check if the instance size was changed by an earlier start
            org_instance_size_from_tag = self._instance_.get("Tags", {}).get(TAG_ORIGINAL_INST_SIZE, None)
            if org_instance_size_from_tag is not None and org_instance_size_from_tag != self.instance_size:
                self._logger_.info("Resizing instance to original size {}", self.instance_size)
                self.instance_size = org_instance_size_from_tag

                # back to original size
                self._resize_instance()
                # delete tag that stored the original state as we have now restored the state of the instance
                self.ec2_client.delete_tags_with_retries(Resources=[self.instance_id],
                                                         Tags=[{"Key": TAG_ORIGINAL_INST_SIZE}])

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_INSTANCE_STOP_ACTION, self.instance_id, self._task_)

        set_original_instance_size()

        instance_sizes = self.resizing_data.get(self.instance_size, [])
        instance_size_index = -1
        original_size_tag_set = False

        while True:

            try:
                self._test_simulate_insufficient_instance_capacity()
                self._start_instance()
                break

            except ClientError as ex:

                # no capacity for this size
                if self.insufficient_capacity(ex):

                    # try to set alternative size if these were specified for the original size
                    self._logger_.warning(WARN_NO_TYPE_CAPACITY, self.instance_size)

                    instance_size_index += 1

                    if instance_size_index >= len(instance_sizes):
                        # out of alternatives
                        # delete tag that stored the original state as we have now restored the state of the instance
                        if original_size_tag_set:
                            self.ec2_client.delete_tags_with_retries(Resources=[self.instance_id],
                                                                     Tags=[
                                                                         {
                                                                             "Key": TAG_ORIGINAL_INST_SIZE
                                                                         }
                                                                     ])

                        raise ex

                    previous_size = self.instance_size
                    self.instance_size = instance_sizes[instance_size_index]
                    # set size to next alternative size from list
                    self._resize_instance()

                    # restore the original size of the instance so next time it is started by this action it can be restored
                    if not original_size_tag_set:
                        self.ec2_client.create_tags_with_retries(Resources=[self.instance_id],
                                                                 Tags=tag_key_value_list({TAG_ORIGINAL_INST_SIZE: previous_size}))
                        original_size_tag_set = True

                    self._logger_.info(INF_RETRY_START, self.instance_id, instance_sizes[instance_size_index])

            except Exception as ex:
                raise Exception(ERR_STARTING.format(self.instance_id, str(ex)))

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StartedInstances=1
        )

        return self.result
