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

import handlers.ec2_state_event_handler
import handlers.ec2_tag_event_handler
import services.ec2_service
from actions import *
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries
from helpers import safe_json
from outputs import raise_exception, raise_value_error

COMMAND_COMMENT = "Command executed by task {}, id {}"
COMMANDS_RESULTS_BUCKET = "Results"

ERR_ADDING_TAGS = "Error setting tags to instance {}, {}"
ERR_BOTH_COMMANDS_EMPTY = "{} and {} parameters cannot be both empty"
ERR_EXECUTING_COMMAND = "Error executing commands on instance {}, {}"
ERR_NO_LINUX_COMMANDS = "Instance {} is a Linux host, but no Linux shell commands are configured for task {}"
ERR_NO_WINDOWS_COMMANDS = "Instance {} is a Windows host, but no Windows PowerShell commands are configured for task {}"
ERR_TAG_TRIGGERED_LOOP_BY_TASK_NAME = \
    "Can not set instance tags {} to instance {} as the create tag event would trigger a loop execution for this " \
    "task with task list \"{}\" in tag {}."
ERR_TAG_TRIGGERED_LOOP_WITH_FILTER = \
    "Can not set instance tags {} to instance {} as the create tag event for the instance tags {} would trigger a " \
    "loop execution for this task with tag filter {}."

WARN_NO_OUTPUT_READ = "No could be read from output in object {} in bucket {}, {}"

GROUP_TITLE_COMMANDS = "Commands"
GROUP_TITLE_TAGGING = "Tagging"

INF_S3_DELETED = "Deleted {} result for command id {} from bucket {} key {}"
INF_S3_READ_OUTPUT = "Output lines at {} for command id {} read from bucket {} key {} are {}"
INF_STATUS_WAITING = "Command {} not completed yet, waiting"
INFO_ADDED_TAGS = "Added tags {} to instance {}"

PARAM_DESC_TAGS_FAILURE = "Tags to add to the instance if the commands did not complete successfully. Don't use tag updates " \
                          " with a tag filter that could trigger a new execution of this task."
PARAM_DESC_TAGS_SUCCESS = "Tags to add to the instance if the commands did complete successfully. Don't use tag updates " \
                          " with a tag filter that could trigger a new execution of this task."
PARAM_DESC_LINUX_COMMANDS = "List of commands to execute on Linux instances."
PARAM_DESC_WINDOWS_COMMANDS = "List of PowerShell commands to execute on Windows Instances"

PARAM_LABEL_SUCCESS = "Instance tags success"
PARAM_LABEL_TAGS_FAILURE = "Instance tags failure"
PARAM_LABEL_LINUX_COMMANDS = "Linux commands"
PARAM_LABEL_WINDOWS_COMMANDS = "Windows PowerShell commands"

PARAM_LINUX_COMMANDS = "LinuxCommands"
PARAM_TAGS_FAILURE = "InstanceTagsFailure"
PARAM_TAGS_SUCCESS = "InstanceTagsSuccess"
PARAM_WINDOWS_COMMANDS = "WindowsCommands"

TAG_PLACEHOLDER_COMMAND = "command-id"

WARN_S3_DELETE_RESULT = "Error deleting result for command id {} from bucket {} with key {}, {}"

STATUS_CANCELLED = "Canceled"
STATUS_CANCELLING = "Cancelling"
STATUS_DELAYED = "Delayed"
STATUS_FAILED = "Failed"
STATUS_IN_PROGRESS = "InProgress"
STATUS_PENDING = "Pending"
STATUS_SUCCESS = "Success"
STATUS_TIMED_OUT = "TimedOut"

STATES_WAITING = [
    STATUS_PENDING,
    STATUS_IN_PROGRESS,
    STATUS_CANCELLING,
    STATUS_DELAYED
]

STATES_SUCCESS = [
    STATUS_SUCCESS
]

STATES_FAILED = [
    STATUS_CANCELLED,
    STATUS_TIMED_OUT,
    STATUS_FAILED
]


class SsmRunCommandAction(ActionEc2EventBase):
    properties = {
        ACTION_TITLE: "SSM Run Command",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPTION: "Executes Linux shell or Windows PowerShell script",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "0f998a42-ea11-46c6-a666-5a127ee76a99",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_SELECT_EXPRESSION: "Reservations[*].Instances[].{InstanceId:InstanceId, Tags:Tags, State:State.Name, "
                                  "Windows:Platform=='windows'}|[?State=='running']",

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: [
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING]
            },
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {
            PARAM_LINUX_COMMANDS: {
                PARAM_DESCRIPTION: PARAM_DESC_LINUX_COMMANDS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_LINUX_COMMANDS
            },
            PARAM_WINDOWS_COMMANDS: {
                PARAM_DESCRIPTION: PARAM_DESC_WINDOWS_COMMANDS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_WINDOWS_COMMANDS
            },
            PARAM_TAGS_SUCCESS: {
                PARAM_DESCRIPTION: PARAM_DESC_TAGS_SUCCESS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SUCCESS
            },
            PARAM_TAGS_FAILURE: {
                PARAM_DESCRIPTION: PARAM_DESC_TAGS_FAILURE,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_TAGS_FAILURE
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_COMMANDS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_LINUX_COMMANDS,
                    PARAM_WINDOWS_COMMANDS
                ]
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_TAGS_SUCCESS,
                    PARAM_TAGS_FAILURE
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "ssm:SendCommand",
            "ssm:GetCommandInvocation",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionEc2EventBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_
        self.command_timeout = self.get(ACTION_PARAM_TIMEOUT) * 60

        self.instance_id = self.instance["InstanceId"]

        self._ssm_client = None
        self._s3_client = None
        self._ec2_client = None

        self.linux_commands = self.get(PARAM_LINUX_COMMANDS, [])
        self.windows_commands = self.get(PARAM_WINDOWS_COMMANDS, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
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
    def ssm_client(self):
        if self._ssm_client is None:
            methods = ["send_command",
                       "list_commands",
                       "get_command_invocation"]
            self._ssm_client = get_client_with_retries("ssm", methods, region=self.instance["Region"],
                                                       session=self._session_, logger=self._logger_)

        return self._ssm_client

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "create_tags",
                "delete_tags"
            ]

            self._ec2_client = get_client_with_retries("ec2", methods,
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        linux_commands = parameters.get(PARAM_LINUX_COMMANDS, [])
        windows_commands = parameters.get(PARAM_WINDOWS_COMMANDS, [])

        if len(linux_commands) == 0 and len(windows_commands) == 0:
            raise_value_error(ERR_BOTH_COMMANDS_EMPTY, PARAM_LINUX_COMMANDS, PARAM_WINDOWS_COMMANDS)

        ActionEc2EventBase.check_tag_filters_and_tags(parameters, task_settings, [PARAM_TAGS_SUCCESS, PARAM_TAGS_FAILURE], logger)

        return parameters

    def _create_instance_tags(self, tag_param, cmd_id):

        if tag_param is None or len(tag_param) == 0:
            return

        tags = self.build_tags_from_template(tag_param, tag_variables={
            TAG_PLACEHOLDER_COMMAND: cmd_id
        })
        self.set_ec2_instance_tags_with_event_loop_check(client=self.ec2_client, instance_ids=[self.instance_id], tags_to_set=tags)

    def is_completed(self, start_data):

        command_id = start_data["command-id"]

        try:
            resp = self.ssm_client.get_command_invocation_with_retries(CommandId=command_id,
                                                                       InstanceId=self.instance_id,
                                                                       _expected_boto3_exceptions_=["InvocationDoesNotExist"])
        except Exception as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code") == "InvocationDoesNotExist":
                return None
            raise Exception("Error retrieving status for command {}, {}".format(command_id, ex))

        self._logger_.debug(safe_json(resp, indent=3))

        status = resp.get("Status")

        self._logger_.info("Status of command {} is {}", command_id, status)

        stdout = resp.get("StandardOutputContent", "")
        stderr = resp.get("StandardErrorContent", "")

        self.result.update({
            "status": status,
            "stdout": stdout,
            "stderr": stderr
        })

        if status in STATES_WAITING:
            self._logger_.info(INF_STATUS_WAITING, command_id)
            return None

        if status in STATES_FAILED or (status == STATUS_SUCCESS and len(stderr) > 0):
            try:
                self._create_instance_tags(PARAM_TAGS_FAILURE, command_id)
            except Exception as ex:
                self._logger_.error(str(ex))

            raise Exception(safe_json(self.result))

        if status in STATES_SUCCESS:
            try:
                self._create_instance_tags(PARAM_TAGS_SUCCESS, command_id)
            except Exception as ex:
                self._logger_.error(str(ex))

            return self.result

        return None

    def execute(self):

        def run_linux_commands():

            resp = self.ssm_client.send_command_with_retries(TimeoutSeconds=self.command_timeout,
                                                             Comment=COMMAND_COMMENT.format(self._task_, self._task_id_)[0:100],
                                                             InstanceIds=[self.instance_id],
                                                             DocumentName="AWS-RunShellScript",
                                                             Parameters={"commands": self.linux_commands})
            # OutputS3BucketName=self.result_bucket)

            return resp["Command"]["CommandId"]

        def run_windows_commands():

            resp = self.ssm_client.send_command_with_retries(TimeoutSeconds=self.command_timeout,
                                                             Comment=COMMAND_COMMENT.format(self._task_, self._task_id_),
                                                             InstanceIds=[self.instance_id],
                                                             DocumentName="AWS-RunPowerShellScript",
                                                             Parameters={"commands": self.windows_commands})
            return resp["Command"]["CommandId"]

        try:
            if self.instance["Windows"]:
                if len(self.windows_commands) == 0:
                    raise_exception(ERR_NO_WINDOWS_COMMANDS, self.instance_id, self._task_)
                command_id = run_windows_commands()
            else:
                if len(self.linux_commands) == 0:
                    raise_exception(ERR_NO_LINUX_COMMANDS, self.instance_id, self._task_)
                command_id = run_linux_commands()

            self.result["command-id"] = command_id

        except Exception as ex:
            raise_exception(ERR_EXECUTING_COMMAND, self.instance_id, ex)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            Commands=1
        )

        return self.result
