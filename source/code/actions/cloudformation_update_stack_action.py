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
from outputs import raise_exception, raise_value_error
from tagging import tag_key_value_list

WARN_INVALID_ROLE_DELETING_STACK = "{} is not a valid role arn to update stack"

STATES_COMPLETED = ["UPDATE_COMPLETE"]
STATES_WAITING = [
    "UPDATE_IN_PROGRESS",
    "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
    "REVIEW_IN_PROGRESS"]

STATES_FAILED = [
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

ERR_CHECKING_COMPLETION = "Error checking completion for update of stack {}, {}"
ERR_STACK_CREATION_FAILED = "Update of stack {} with stack id  failed, {}"
ERR_STACK_DOES_NOT_EXIST = "Stack {} does not exist"
ERR_START_UPDATING_STACK = "Error starting updating of stack, {}"
ERR_UNKNOWN_STACK_STATE = "Unknown state \"{}\" for stack {} with id {}"
ERR_BUCKET_AND_EMPTY_KEY = "{} can not be empty if {} is specified"
ERR_GENERATE_URL_TEMPLATE = "Error generate a signed url for template to update stack from bucket {} with key {}, {}"
ERR_GENERATE_URL_POLICY = "Error generate as signed url for update policy for updating stack from bucket {} with key {}, {}"
ERR_TEMPLATE_OR_PARAMETERS_OR_TAGS_MUST_BE_SPECIFIED = "Stack template {}, parameters {} or tags {} must be specified for " \
                                                       "stack update"

INF_STACK_UPDATE_STARTED = "Update of stack {} started, stack id is {}"
INF_START_CHECK_STACK_COMPLETION = "Checking completion status of stack {}"
INF_START_UPDATING_STACK = "Updating stack {} with parameters {}"
INF_WAIT_FOR_STACK_TO_COMPLETE = "Waiting for stack creation to complete"

PARAM_GROUP_STACK_TITLE = "Stack creation options"

PARAM_STACK_NAME = "StackName"
PARAM_TEMPLATE_BUCKET = "TemplateBucket"
PARAM_TEMPLATE_KEY = "TemplateKey"
PARAM_ON_FAILURE = "OnFailure"
PARAM_STACK_PARAMETERS = "StackParameters"
PARAM_TAGS = "StackTags"
PARAM_CAPABILITY_IAM = "CapabilityIam"
PARAM_ROLE_ARN_LIST = "RoleArnList"
PARAM_STACK_POLICY_BUCKET = "StackPolicyBucket"
PARAM_STACK_OVERRIDE_POLICY_KEY = "StackPolicyDuringUpgradeKey"

PARAM_LABEL_STACK_NAME = "Stack name"
PARAM_LABEL_TEMPLATE_BUCKET = "S3 template bucket"
PARAM_LABEL_TEMPLATE_KEY = "S3 template key"
PARAM_LABEL_ON_FAILURE = "On failure action"
PARAM_LABEL_PARAMETERS = "Stack Parameters"
PARAM_LABEL_TAGS = "Tags"
PARAM_LABEL_CAPABILITY_IAM = "Requires IAM Capability"
PARAM_LABEL_ROLE_ARN_LIST = "Update Role ARN list"
PARAM_LABEL_STACK_POLICY_BUCKET = "S3 policy bucket"
PARAM_LABEL_STACK_OVERRIDE_POLICY_KEY = "S3 temporary override policy key"

PARAM_DESC_STACK_NAME = \
    "Name of an existing stack to update."

PARAM_DESC_ON_FAILURE = \
    "Determines what action will be taken if stack creation fails."

PARAM_DESC_PARAMETERS = \
    "Comma separated list of parameters in parameter-name=parameter-value format." \
    "For parameters that are not in this list the existing value of these parameters are used"

PARAM_DESC_TAGS = \
    "Comma separated list of parameters in tag-name=tag-value format."

PARAM_DESC_CAPABILITY_IAM = \
    "Set to True to acknowledge the creation of IAM resources in the stack."

PARAM_DESC_ROLE_ARN_LIST = "List of Amazon Resource Names (ARN) of an AWS Identity and Access Management (IAM) roles that AWS " \
                           "CloudFormation assumes to create the stack. The account numbers of in the ARN of the roles are " \
                           "used to find a match between a role and the accounts the task is executed for. If a match is found, " \
                           "then the cross account role is used to create the stack for the account. This role must have the " \
                           "required permissions to update the resources of the stack."

PARAM_DESC_TEMPLATE_BUCKET = \
    "Name of the S3 bucket for the stack template."

PARAM_DESC_TEMPLATE_KEY = \
    "Key of the S3 object for the stack template. The role running the Ops Automator must have permissions to read this object. " \
    "Leave this parameter empty to use the existing template of the stack."

PARAM_DESC_STACK_POLICY_BUCKET = \
    "Name of the S3 bucket for the stack update policy, leave blank to use the template bucket."

PARAM_DESC_STACK_OVERRIDE_POLICY_KEY = \
    "Key of the S3 object containing the temporary overriding stack policy. The role running the Ops Automator must " \
    "have permissions to read this object."


class CloudformationUpdateStackAction(ActionBase):
    properties = {

        ACTION_TITLE: "Update CloudFormation Stack",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Updates an existing CloudFormation stack",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "820224c3-5be0-4432-9ef6-e90276bd8b04",

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
                PARAM_DEFAULT: "",
                PARAM_REQUIRED: False
            },
            PARAM_TEMPLATE_KEY: {
                PARAM_LABEL: PARAM_LABEL_TEMPLATE_KEY,
                PARAM_DESCRIPTION: PARAM_DESC_TEMPLATE_KEY,
                PARAM_TYPE: str,
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
            PARAM_STACK_OVERRIDE_POLICY_KEY: {
                PARAM_LABEL: PARAM_LABEL_STACK_OVERRIDE_POLICY_KEY,
                PARAM_DESCRIPTION: PARAM_DESC_STACK_OVERRIDE_POLICY_KEY,
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
                    PARAM_STACK_POLICY_BUCKET,
                    PARAM_STACK_OVERRIDE_POLICY_KEY,
                ]
            }
        ],

        ACTION_PERMISSIONS: ["cloudformation:UpdateStack",
                             "cloudformation:DescribeStacks",
                             "cloudformation:DescribeStackEvents",
                             "cloudformation:GetTemplateSummary",
                             "cloudformation:SetStackPolicy",
                             "iam:PassRole"]
    }

    @staticmethod
    def action_validate_parameters(parameters, _, __):

        no_value = ["", None]
        policy_bucket = parameters.get(PARAM_STACK_POLICY_BUCKET, "")
        policy_key = parameters.get(PARAM_STACK_OVERRIDE_POLICY_KEY, "")

        if policy_key in no_value and policy_bucket not in no_value:
            raise_value_error(ERR_BUCKET_AND_EMPTY_KEY, PARAM_STACK_OVERRIDE_POLICY_KEY, PARAM_STACK_POLICY_BUCKET)

        template_bucket = parameters.get(PARAM_TEMPLATE_BUCKET, "")
        template_key = parameters.get(PARAM_TEMPLATE_KEY, "")

        if template_key in no_value and template_bucket not in no_value:
            raise_value_error(ERR_BUCKET_AND_EMPTY_KEY, PARAM_TEMPLATE_KEY, PARAM_TEMPLATE_BUCKET)

        if template_key in no_value and parameters.get(PARAM_STACK_PARAMETERS) in no_value and parameters.get(
                PARAM_TAGS) in no_value:
            raise_value_error(
                ERR_TEMPLATE_OR_PARAMETERS_OR_TAGS_MUST_BE_SPECIFIED.format(PARAM_TEMPLATE_KEY, PARAM_STACK_PARAMETERS, PARAM_TAGS))

        return parameters

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
                       "update_stack",
                       "describe_stack_events",
                       "get_template_summary"]

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
        self._capability_iam_ = None

        ActionBase.__init__(self, action_args, action_parameters)

        self.time_resource = self._resources_

        self.role_arn_list = self.get(PARAM_ROLE_ARN_LIST, [])
        self.stack_policy_key = self.get(PARAM_STACK_OVERRIDE_POLICY_KEY, None)

        self._s3_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

        self._cfn_client = None

    def is_completed(self, update_data):

        self._logger_.debug("Checking status, start result data is {}", safe_json(update_data, indent=3))
        stack_name = update_data["stack-name"]
        stack_id = update_data["stack-id"]

        self._logger_.info(INF_START_CHECK_STACK_COMPLETION, stack_name)

        stacks = []

        try:
            cfn = services.create_service("cloudformation", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("cloudformation",
                                                                                            context=self._context_))

            stacks = [s for s in cfn.describe(services.cloudformation_service.STACKS,
                                              StackName=stack_name,
                                              region=self._region_) if
                      s["StackId"] == stack_id]

        except ClientError as ce:
            if ce.response["Error"]["Message"].endswith("{} does not exist".format(self._stack_name_)):
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

            raise_exception(ERR_STACK_CREATION_FAILED, stack_name, update_data["stack-id"])

        if stack_state in STATES_COMPLETED:
            self.result["stack-name"] = stack_name
            self.result["stack-id"] = stack["StackId"]
            self.result["update-time"] = stack["LastUpdatedTime"]
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
                                                         Params={
                                                             "Bucket": bucket,
                                                             "Key": key
                                                         },
                                                         ExpiresIn=3600)

        def get_update_role():
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
                    self._logger_.warning(WARN_INVALID_ROLE_DELETING_STACK)
            return account_role_arn

        def get_stack_parameter_values():
            stacks = self.cfn_client.describe_stacks_with_retries(StackName=self._stack_name_).get("Stacks", [])
            return {t["ParameterKey"]: t["ParameterValue"] for t in stacks[0].get("Parameters")} if len(stacks) != 0 else {}

        def get_template_parameters(url):
            return [prm["ParameterKey"] for prm in self.cfn_client.get_template_summary(TemplateURL=url).get("Parameters", [])]

        self.result["stack-name"] = self._stack_name_

        args = {
            "StackName": self._stack_name_
        }

        template_parameters = []
        if self.get(PARAM_TEMPLATE_KEY, None) not in [None, ""]:
            try:
                template_url = get_signed_s3_url(self._template_bucket_, self._template_key_)
                args["TemplateURL"] = template_url
                template_parameters = get_template_parameters(template_url)
            except Exception as ex:
                raise_exception(ERR_GENERATE_URL_TEMPLATE, self._template_bucket_, self._template_key_, ex)
        else:
            args["UsePreviousTemplate"] = True
            template_parameters = None

        if self._capability_iam_:
            args["Capabilities"] = ["CAPABILITY_IAM"]

        role_arn = get_update_role()
        if role_arn is not None:
            self._logger_.info("Using role {} as update Role", role_arn)
            args["RoleARN"] = role_arn

        if self.policy_bucket is not None and self.stack_policy_key is not None:
            try:
                policy_url = get_signed_s3_url(self.policy_bucket, self.stack_policy_key)
                args["StackPolicyDuringUpdateURL"] = policy_url
            except Exception as ex:
                raise_exception(ERR_GENERATE_URL_POLICY, self.policy_bucket, self.stack_policy_key, ex)

        tags = self.build_tags_from_template(parameter_name=PARAM_TAGS, include_deleted_tags=False)
        if len(tags) > 0:
            args["Tags"] = tag_key_value_list(tags)

        update_params = self.build_tags_from_template(PARAM_STACK_PARAMETERS)
        if len(update_params) > 0:

            current_params = get_stack_parameter_values()

            params = []
            for p in template_parameters if template_parameters is not None else current_params:
                if p in update_params:
                    if update_params[p] != current_params[p]:
                        params.append({"ParameterKey": p, "ParameterValue": update_params[p]})
                    else:
                        params.append({"ParameterKey": p, "UsePreviousValue": True})

            if len(params) != 0:
                args["Parameters"] = params

        try:
            self._logger_.info(INF_START_UPDATING_STACK, self._stack_name_, safe_json(args, indent=3))

            resp = self.cfn_client.update_stack_with_retries(**args)

            self._logger_.debug("update_stack result {}", safe_json(resp, indent=3))
            stack_id = resp["StackId"]
            self._logger_.info(INF_STACK_UPDATE_STARTED, self._stack_name_, stack_id)

            self.result["stack-id"] = stack_id
            self.result[METRICS_DATA] = build_action_metrics(self, UpdatedStacks=1)

            return self.result

        except Exception as ex:
            raise_exception(ERR_START_UPDATING_STACK, ex)
