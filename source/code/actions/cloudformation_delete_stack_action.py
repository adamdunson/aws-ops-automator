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


from botocore.exceptions import ClientError

import services.cloudformation_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from helpers import safe_json
from outputs import raise_exception

WARN_INVALID_ROLE_DELETE_STACK = "{} is not a valid role arn for deleting stack"

ERR_CHECKING_DELETION_OF_STACK_ = "Error checking completion of deleting stack {}, {}"
ERR_DELETING_STACK_ = "Error deleting stack {}, {}"
ERR_DELETING_FAILED = "Deleting stack {} failed"

INF_DELETE_STACK_STARTED = "Deletion of stack {} started"
INF_START_DELETING_STACK = "Deleting stack {}"
INF_WAITING_FOR_COMPLETION__ = "Waiting for completion.."
INF_STACK_HAS_BEEN_DELETED = "Stack {} has been deleted"

PARAM_GROUP_STACK_TITLE = "Stack deletion options"

PARAM_ROLE_ARN_LIST = "RoleArnList"
PARAM_LABEL_ROLE_ARN_LIST = "Delete Role ARN list"
PARAM_DESC_ROLE_ARN_LIST =  "List of Amazon Resource Names (ARN) of an AWS Identity and Access Management (IAM) roles that AWS " \
                           "CloudFormation assumes to create the stack. The account numbers of in the ARN of the roles are " \
                           "used to find a match between a role and the accounts the task is executed for. If a match is found, " \
                           "then the cross account role is used to create the stack for the account. This role must have the " \
                           "required permissions to delete the resources of the stack."


class CloudformationDeleteStackAction(ActionBase):
    properties = {

        ACTION_TITLE: "Delete CloudFormation Stack",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes an existing CloudFormation stack",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6d6a5ed9-3d39-466d-a663-0585907bcdb3",

        ACTION_SERVICE: "cloudformation",
        ACTION_RESOURCES: services.cloudformation_service.STACKS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_CROSS_ACCOUNT: True,
        ACTION_INTERNAL: False,
        ACTION_MULTI_REGION: True,

        ACTION_SELECT_EXPRESSION:
            "Stacks[*].{StackName:StackName, Tags:Tags, StackStatus:StackStatus}"
            "|[?contains(['CREATE_COMPLETE','UPDATE_COMPLETE', 'CREATE_FAILED', 'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE',"
            "'DELETE_FAILED' ],StackStatus)]",

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_PARAMETERS: {

            PARAM_ROLE_ARN_LIST: {
                PARAM_LABEL: PARAM_LABEL_ROLE_ARN_LIST,
                PARAM_DESCRIPTION: PARAM_DESC_ROLE_ARN_LIST,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: PARAM_GROUP_STACK_TITLE,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ROLE_ARN_LIST
                ],
            }
        ],

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_PERMISSIONS: ["cloudformation:DeleteStack",
                             "cloudformation:DescribeStacks",
                             "iam:PassRole"]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.stack = self._resources_
        self.stack_name = self.stack["StackName"]

        self.role_arn_list = self.get(PARAM_ROLE_ARN_LIST, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "stack-name": self.stack_name,
            "task": self._task_
        }

        self._cfn_client = None

    @property
    def cfn_client(self):
        if self._cfn_client is None:
            self._cfn_client = get_client_with_retries("cloudformation",
                                                       methods=["describe_stacks", "delete_stack"],
                                                       context=self._context_,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._cfn_client

    @staticmethod
    def action_logging_subject(arguments, _):
        stack = arguments[ACTION_PARAM_RESOURCES]["StackName"]
        time_resource = arguments[ACTION_PARAM_RESOURCES]
        region = time_resource["Region"]
        account = time_resource["AwsAccount"]
        return "{}-{}-{}-{}".format(account, region, stack, log_stream_date())

    def is_completed(self, _):

        stack_state = None
        try:
            response = self.cfn_client.describe_stacks_with_retries(StackName=self.stack_name)
            stack = response["Stacks"][0]
            stack_state = stack["StackStatus"]
            self._logger_.info("State of stack is {}", stack_state)
            self._logger_.debug("Stack info is {}", safe_json(stack, indent=3))
        except ClientError as ce:
            if ce.response["Error"]["Message"].endswith("{} does not exist".format(self.stack_name)):
                self._logger_.info(INF_STACK_HAS_BEEN_DELETED, self.stack_name)
                return self.result
            else:
                raise_exception(ERR_CHECKING_DELETION_OF_STACK_, self.stack_name, ce)
        except Exception as ex:
            raise_exception(ERR_CHECKING_DELETION_OF_STACK_, self.stack_name, ex)

        if stack_state == "DELETE_FAILED":
            raise_exception(ERR_DELETING_FAILED, self.stack_name)

        self._logger_.info(INF_WAITING_FOR_COMPLETION__)
        return None

    def execute(self):

        def get_delete_role():
            account_role_arn = None
            assumed_role = self.get(ACTION_PARAM_ASSUMED_ROLE, None)
            if assumed_role is not None:
                account = services.account_from_role_arn(assumed_role)
            else:
                account = services.get_aws_account()
            for arn in self.role_arn_list:
                try:
                    if account == services.account_from_role_arn(arn):
                        account_role_arn = arn
                        break
                except ValueError:
                    self._logger_.warning(WARN_INVALID_ROLE_DELETE_STACK)
            return account_role_arn

        try:
            self._logger_.info(INF_START_DELETING_STACK, self.stack_name)

            args = {
                "StackName": self.stack_name
            }

            delete_role_arn = get_delete_role()
            if delete_role_arn is not None:
                args["RoleARN"] = delete_role_arn

            self.cfn_client.delete_stack_with_retries(**args)

            self._logger_.info(INF_DELETE_STACK_STARTED, self.stack_name)

            self.result[METRICS_DATA] = build_action_metrics(self, DeletedStacks=1)

            return self.result

        except Exception as ex:
            raise_exception(ERR_DELETING_STACK_, self.stack_name, ex)
