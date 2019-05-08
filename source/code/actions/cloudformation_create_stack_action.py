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

import services
import services.cloudformation_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging import tag_key_value_list

TEMPLATE_BUCKET = "Templates"

STATES_COMPLETED = ["CREATE_COMPLETE",
                    "UPDATE_COMPLETE"]
STATES_WAITING = [
    "CREATE_IN_PROGRESS",
    "UPDATE_IN_PROGRESS",
    "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
    "REVIEW_IN_PROGRESS"]

STATES_FAILED = [
    "CREATE_FAILED",
    "ROLLBACK_IN_PROGRESS",
    "ROLLBACK_FAILED",
    "ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_IN_PROGRESS",
    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
    "UPDATE_ROLLBACK_COMPLETE",
    "DELETE_IN_PROGRESS",
    "DELETE_FAILED",
    "DELETE_COMPLETE"]

ERR_CHECKING_COMPLETION = "Error checking completion creation of stack {}, {}"
ERR_STACK_CREATION_FAILED = "Creation of stack {} with stack id  failed, {}"
ERR_STACK_DOES_NOT_EXIST = "Stack {} does not exist"
ERR_STACK_DOES_NOT_EXIST_DELETED = "Stack does {} not exist, it may be deleted because its creation failed"
ERR_START_CREATING_STACK = "Error starting creation of stack, {}"
ERR_UNKNOWN_STACK_STATE = "Unknown state \"{}\" for stack {} with id {}"
ERR_GENERATE_URL_TEMPLATE = "Error generate a signed url for stack create template from bucket {} with key {}, {}"
ERR_GENERATE_URL_POLICY = "Error generate as signed url create policy for creating stack from bucket {} with key {}, {}"

WARN_VALID_ROLE_ARN = "{} is not a valid role arn"

INF_STACK_CREATION_STARTED = "Creation of stack {} started, stack id is {}"
INF_START_CHECK_STACK_COMPLETION = "Checking completion status of stack {}"
INF_START_CREATING_STACK = "Creating stack {} with parameters {}"
INF_WAIT_FOR_STACK_TO_COMPLETE = "Waiting for stack creation to complete"

PARAM_GROUP_STACK_TITLE = "Stack creation options"

PARAM_CAPABILITY_IAM = "CapabilityIam"
PARAM_ON_FAILURE = "OnFailure"
PARAM_ROLE_ARN_LIST = "RoleArnList"
PARAM_STACK_NAME = "StackName"
PARAM_STACK_PARAMETERS = "StackParameters"
PARAM_STACK_POLICY_BUCKET = "StackPolicyBucket"
PARAM_STACK_POLICY_KEY = "StackPolicyKey"
PARAM_TAGS = "StackTags"
PARAM_TEMPLATE_BUCKET = "TemplateBucket"
PARAM_TEMPLATE_KEY = "TemplateKey"

PARAM_LABEL_STACK_NAME = "Stack name"
PARAM_LABEL_TEMPLATE_BUCKET = "S3 template bucket"
PARAM_LABEL_TEMPLATE_KEY = "S3 template key"
PARAM_LABEL_ON_FAILURE = "On failure action"
PARAM_LABEL_PARAMETERS = "Stack Parameters"
PARAM_LABEL_TAGS = "Tags"
PARAM_LABEL_CAPABILITY_IAM = "Requires IAM Capability"
PARAM_LABEL_ROLE_ARN_LIST = "Create Role ARN list"
PARAM_LABEL_STACK_POLICY_BUCKET = "S3 policy bucket"
PARAM_LABEL_STACK_POLICY_KEY = "S3 policy key"

PARAM_DESC_STACK_NAME = \
    "Name of the created stack."

PARAM_DESC_ON_FAILURE = \
    "Determines what action will be taken if stack creation fails."

PARAM_DESC_PARAMETERS = \
    "Comma separated list of parameters in parameter-name=parameter-value format."

PARAM_DESC_TAGS = \
    "Comma separated list of parameters in tag-name=tag-value format. " \
    "Standard placeholders can be used to provide tag names or values."

PARAM_DESC_CAPABILITY_IAM = \
    "Set to acknowledge the creation of IAM resources in the stack."

PARAM_DESC_ROLE_ARN_LIST = "List of Amazon Resource Names (ARN) of an AWS Identity and Access Management (IAM) roles that AWS " \
                           "CloudFormation assumes to create the stack. The account numbers of in the ARN of the roles are " \
                           "used to find a match between a role and the accounts the task is executed for. If a match is found, " \
                           "then the cross account role is used to create the stack for the account. This role must have the " \
                           "required permissions to create the resources of the stack."

PARAM_DESC_TEMPLATE_BUCKET = \
    "Name of the S3 bucket to read the stack template from."

PARAM_DESC_TEMPLATE_KEY = \
    "Key of the S3 object for the stack template. The role running the Ops Automator must have permissions to read this object " \
    "from the bucket."

PARAM_DESC_STACK_POLICY_BUCKET = \
    "Name of the S3 bucket to read the stack create policy object from, if empty the template bucket will be used."

PARAM_DESC_STACK_POLICY_KEY = \
    "Key of the S3 object for the stack policy. The role running the Ops Automator must have permissions to read this " \
    "object from the bucket."


class CloudformationCreateStackAction(ActionBase):
    properties = {

        ACTION_TITLE: "Create CloudFormation Stack",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Creates a cloudformation stack",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "5429419b-26b5-4c35-9a8b-b25463a12bb0",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_CROSS_ACCOUNT: True,
        ACTION_INTERNAL: False,
        ACTION_MULTI_REGION: True,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_EXECUTE_SIZE: [
            ACTION_SIZE_STANDARD],

        ACTION_COMPLETION_SIZE: [
            ACTION_SIZE_STANDARD],

        ACTION_PARAMETERS: {

            PARAM_STACK_NAME: {
                PARAM_LABEL: PARAM_LABEL_STACK_NAME,
                PARAM_DESCRIPTION: PARAM_DESC_STACK_NAME,
                PARAM_TYPE: str,
                PARAM_MAX_LEN: 128,
                PARAM_REQUIRED: True
            },
            PARAM_TEMPLATE_BUCKET: {
                PARAM_LABEL: PARAM_LABEL_TEMPLATE_BUCKET,
                PARAM_DESCRIPTION: PARAM_DESC_TEMPLATE_BUCKET,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True
            },
            PARAM_TEMPLATE_KEY: {
                PARAM_LABEL: PARAM_LABEL_TEMPLATE_KEY,
                PARAM_DESCRIPTION: PARAM_DESC_TEMPLATE_KEY,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True
            },
            PARAM_ON_FAILURE: {
                PARAM_LABEL: PARAM_LABEL_ON_FAILURE,
                PARAM_DESCRIPTION: PARAM_DESC_ON_FAILURE,
                PARAM_TYPE: str,
                PARAM_ALLOWED_VALUES: ["DO_NOTHING", "ROLLBACK", "DELETE"],
                PARAM_DEFAULT: "ROLLBACK",
                PARAM_REQUIRED: False
            },
            PARAM_STACK_PARAMETERS: {
                PARAM_LABEL: PARAM_LABEL_PARAMETERS,
                PARAM_DESCRIPTION: PARAM_DESC_PARAMETERS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_TAGS: {
                PARAM_LABEL: PARAM_LABEL_TAGS,
                PARAM_DESCRIPTION: PARAM_DESC_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_CAPABILITY_IAM: {
                PARAM_LABEL: PARAM_LABEL_CAPABILITY_IAM,
                PARAM_DESCRIPTION: PARAM_DESC_CAPABILITY_IAM,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False
            },
            PARAM_ROLE_ARN_LIST: {
                PARAM_LABEL: PARAM_LABEL_ROLE_ARN_LIST,
                PARAM_DESCRIPTION: PARAM_DESC_ROLE_ARN_LIST,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: True
            },
            PARAM_STACK_POLICY_BUCKET: {
                PARAM_LABEL: PARAM_LABEL_STACK_POLICY_BUCKET,
                PARAM_DESCRIPTION: PARAM_DESC_STACK_POLICY_BUCKET,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_STACK_POLICY_KEY: {
                PARAM_LABEL: PARAM_LABEL_STACK_POLICY_KEY,
                PARAM_DESCRIPTION: PARAM_DESC_STACK_POLICY_KEY,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: PARAM_GROUP_STACK_TITLE,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STACK_NAME,
                    PARAM_TEMPLATE_BUCKET,
                    PARAM_TEMPLATE_KEY,
                    PARAM_STACK_PARAMETERS,
                    PARAM_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: "Permissions",
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CAPABILITY_IAM,
                    PARAM_ROLE_ARN_LIST
                ]
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: "Advanced creation options",
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ON_FAILURE,
                    PARAM_STACK_POLICY_BUCKET,
                    PARAM_STACK_POLICY_KEY,
                ]
            }
        ],

        ACTION_PERMISSIONS: ["cloudformation:CreateStack",
                             "cloudformation:DescribeStacks",
                             "cloudformation:DescribeStackEvents",
                             "iam:PassRole"]
    }

    @staticmethod
    def action_logging_subject(arguments, parameters):
        stack = parameters[PARAM_STACK_NAME]
        time_resource = arguments[ACTION_PARAM_RESOURCES]
        region = time_resource["Region"]
        account = time_resource["AwsAccount"]
        return "{}-{}-{}-{}".format(account, region, stack, log_stream_date())

    @property
    def cfn_client(self):
        if self._cfn_client is None:
            methods = ["describe_stacks",
                       "create_stack",
                       "describe_stack_events"]

            self._cfn_client = get_client_with_retries("cloudformation",
                                                       methods,
                                                       context=self._context_,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._cfn_client

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = get_client_with_retries("s3",
                                                      methods=["generate_presigned_url"],
                                                      context=self._context_,
                                                      logger=self._logger_)
        return self._s3_client

    @property
    def policy_bucket(self):
        return self._policy_bucket_ if self._policy_bucket_ not in ["", None] else self._template_bucket_

    def __init__(self, action_args, action_parameters):

        self._stack_name_ = None
        self._template_bucket_ = None
        self._template_key_ = None
        self._policy_bucket_ = None
        self._stack_parameters_ = None
        self._on_failure_ = None
        self._capability_iam_ = None

        ActionBase.__init__(self, action_args, action_parameters)

        self.time_resource = self._resources_

        self.role_arn_list = self.get(PARAM_ROLE_ARN_LIST, [])
        self.stack_policy_key = self.get(PARAM_STACK_POLICY_KEY, None)

        self._s3_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

        self._cfn_client = None

    def is_completed(self, execute_result):

        self._logger_.debug("Checking status, start result data is {}", safe_json(execute_result, indent=3))
        stack_name = execute_result["stack-name"]
        stack_id = execute_result["stack-id"]

        self._logger_.info(INF_START_CHECK_STACK_COMPLETION, stack_name)

        stacks = []

        try:
            cfn = services.create_service("cloudformation",
                                          session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("cloudformation",
                                                                                            context=self._context_))

            stacks = [s for s in cfn.describe(services.cloudformation_service.STACKS,
                                              StackName=stack_name,
                                              region=self._region_) if
                      s["StackId"] == stack_id]

        except ClientError as ce:
            if ce.response["Error"]["Message"].endswith("{} does not exist".format(stack_name)):
                if self._on_failure_ == "DELETE":
                    raise_exception(ERR_STACK_DOES_NOT_EXIST_DELETED, stack_name)
                else:
                    raise_exception(ERR_STACK_DOES_NOT_EXIST, stack_name)
        except Exception as ce:
            raise_exception(ERR_CHECKING_COMPLETION, stack_name, ce)

        if len(stacks) == 0:
            return None

        stack = stacks[0]
        stack_state = stack["StackStatus"]
        self._logger_.info("State of stack is {}", stack_state)

        if stack_state in STATES_FAILED:
            resp = self.cfn_client.describe_stack_events_with_retries(StackName=stack_name)

            self._logger_.error(safe_json(resp.get("StackEvents", []), indent=3))

            raise_exception(ERR_STACK_CREATION_FAILED, stack_name, execute_result["stack-id"])

        if stack_state in STATES_COMPLETED:
            self.result["stack-name"] = stack_name
            self.result["stack-id"] = stack["StackId"]
            self.result["creation-time"] = stack["CreationTime"]
            self.result["tags"] = stack["Tags"]
            self.result["parameters"] = stack["Parameters"] if "Parameters" in stack else []
            return self.result

        if stack_state in STATES_WAITING:
            self._logger_.info(INF_WAIT_FOR_STACK_TO_COMPLETE)
            return None

        self._logger_.error(ERR_UNKNOWN_STACK_STATE, stack_state, stack_name, stack["StackId"])
        return None

    def execute(self):

        def get_signed_s3_url(bucket, key):
            return self.s3_client.generate_presigned_url(ClientMethod="get_object",
                                                         Params={"Bucket": bucket, "Key": key},
                                                         ExpiresIn=3600)

        def get_create_role():
            account_role_arn = None
            account = self.get_account_for_task()
            for arn in self.role_arn_list:
                try:
                    if account == services.account_from_role_arn(arn):
                        account_role_arn = arn
                        break
                except ValueError:
                    self._logger_.warning(WARN_VALID_ROLE_ARN)
            return account_role_arn

        stack_name = self.build_str_from_template(parameter_name=PARAM_STACK_NAME)
        self.result["stack-name"] = stack_name

        template_url = ""
        try:
            template_url = get_signed_s3_url(self._template_bucket_, self._template_key_)
        except Exception as ex:
            raise_exception(ERR_GENERATE_URL_TEMPLATE, self._template_bucket_, self._template_key_, ex)

        args = {
            "StackName": stack_name,
            "OnFailure": self._on_failure_,
            "TemplateURL": template_url,
            "TimeoutInMinutes": self._timeout_
        }

        if self._capability_iam_:
            args["Capabilities"] = ["CAPABILITY_IAM"]

        role_arn = get_create_role()
        if role_arn is not None:
            self._logger_.info("Using role {} as create Role", role_arn)
            args["RoleARN"] = role_arn

        if self.policy_bucket is not None and self.stack_policy_key is not None:
            policy_url = ""
            try:
                policy_url = get_signed_s3_url(self.policy_bucket, self.stack_policy_key)
            except Exception as ex:
                raise_exception(ERR_GENERATE_URL_POLICY, self.policy_bucket, self.stack_policy_key, ex)
            args["StackPolicyURL"] = policy_url

        tags = self.build_tags_from_template(parameter_name=PARAM_TAGS)
        if len(tags) > 0:
            args["Tags"] = tag_key_value_list(tags)

        stack_parameters = self.build_tags_from_template(parameter_name=PARAM_STACK_PARAMETERS)
        if len(stack_parameters) > 0:
            args["Parameters"] = [{"ParameterKey": p, "ParameterValue": stack_parameters[p]} for p in stack_parameters]

        try:
            self._logger_.info(INF_START_CREATING_STACK, stack_name, safe_json(args, indent=3))

            resp = self.cfn_client.create_stack_with_retries(**args)

            self._logger_.debug("create_stack result {}", safe_json(resp, indent=3))
            stack_id = resp["StackId"]
            self._logger_.info(INF_STACK_CREATION_STARTED, stack_name, stack_id)

            self.result["stack-id"] = stack_id
            self.result[METRICS_DATA] = build_action_metrics(self, CreatedStacks=1)

            return self.result

        except Exception as ex:
            raise_exception(ERR_START_CREATING_STACK, ex)
