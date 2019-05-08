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

import json
import re

import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception, raise_value_error
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

DEFAULT_MAX_INSTANCES = 25

IMAGE_STATE_FAILED = "failed"
IMAGE_STATE_PENDING = "pending"
IMAGE_STATE_COMPLETED = "available"

KMS_KEY_ID_PATTERN = r"arn:aws-us-gov:kms:(.)*:key\/([0-9,a-f]){8}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){12}"

COPIED_IMAGES_BOTH = "Both"
COPIED_IMAGES_SHARED_TO_ACCOUNT = "SharedToAccount"
COPIED_OWNED_BY_ACCOUNT = "OwnedByAccount"

TAG_PLACEHOLDER_SOURCE_REGION = "source-region"
TAG_PLACEHOLDER_SOURCE_IMAGE_ID = "source-image-id"
TAG_PLACEHOLDER_COPIED_IMAGE_ID = "copy-image-id"
TAG_PLACEHOLDER_COPIED_NAME = "copy-image-name"
TAG_PLACEHOLDER_COPIED_REGION = "destination-region"
TAG_PLACEHOLDER_OWNER_ACCOUNT = "owner-account"
TAG_PLACEHOLDER_SOURCE_DESCRIPTION = "source-description"
TAG_PLACEHOLDER_SOURCE_NAME = "source-name"
TAG_PLACEHOLDER_SOURCE_INSTANCE = "source-instance-id"

MARKER_TAG_SOURCE_IMAGE_ID_TEMPLATE = "OpsAutomator:{}-Ec2CopyImage-CopiedFromImage"
MARKER_TAG_COPIED_TO_TEMPLATE = "OpsAutomator:{}-{{}}-Ec2CopyImage-CopiedTo"

GROUP_LABEL_IMAGE_COPY_OPTIONS = "Image copy options"
GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS = "Permissions and encryption"

PARAM_DESC_COPIED_IMAGE_TAGS = "Copied tags from source image"
PARAM_DESC_IMAGE_DESCRIPTION = "Description for copied image, leave blank to copy source description"
PARAM_DESC_IMAGE_NAME = "Name for copied image, leave blank to copy source name"
PARAM_DESC_DESTINATION_REGION = "Destination region for copied image"
PARAM_DESC_COPIED_IMAGES = "Select which images are copied. Images owned by the account, shared with the account or both."
PARAM_DESC_IMAGE_TAGS = "Tags to add to copied image"
PARAM_DESC_ACCOUNTS_EXECUTE_PERMISSIONS = \
    "List of account that will be granted access to create instances from the copied image."
PARAM_DESC_CROSS_ACCOUNT_TAG_ROLENAME = \
    "Name of the cross account role in the accounts the image is shared with, that is used to create tags in these accounts " \
    "for the shared image. Leave this parameter empty to use the default role with name \"OpsAutomatorActionsRole\" if it " \
    "exists or \"{}\". The role must give permissions to use the Ec2SetTags action.".format(handlers.default_rolename_for_stack())
PARAM_DESC_KMS_KEY_ID = \
    "The full ARN of the AWS Key Management Service (AWS KMS) CMK to use when creating the image copy. This parameter is only " \
    "required if you want to use a non-default CMK; if this parameter is not specified, the default CMK for EBS is used. " \
    "The ARN contains the arn:aws-us-gov:kms namespace, followed by the region of the CMK, the AWS account ID of the CMK owner, " \
    "the key namespace, and then the CMK ID. The specified CMK must exist in the region that the image is being copied to. " \
    "The account or the role that is used by the Ops Automator, or the cross account role must have been given  permission to " \
    "use the key."
PARAM_DESC_COPY_SHARED_FROM_ACCOUNTS = \
    "Comma separated list of accounts to copy shared images from. Leave blank to copy shared images from all accounts"
PARAM_DESC_ENCRYPTED = "Specifies whether the destination images should be encrypted."
PARAM_DESC_SOURCE_TAGS = "Tags to set to source image after a successful copy"
PARAM_DESC_MAX_INSTANCES = \
    "Max number of tasks running for this task per account ({}-{})"

PARAM_LABEL_ACCOUNTS_EXECUTE_PERMISSIONS = "Accounts with launch permissions"
PARAM_LABEL_COPIED_IMAGE_TAGS = "Copied tags"
PARAM_LABEL_COPIED_IMAGES = "Images copied"
PARAM_LABEL_COPY_SHARED_FROM_ACCOUNTS = "Shared by accounts"
PARAM_LABEL_CROSS_ACCOUNT_TAG_ROLENAME = "Role name for tagging shared images"
PARAM_LABEL_DESTINATION_REGION = "Destination region"
PARAM_LABEL_ENCRYPTED = "Encrypted"
PARAM_LABEL_KMS_KEY_ID = "KMS Key Id"
PARAM_LABEL_IMAGE_DESCRIPTION = "Copied image description"
PARAM_LABEL_IMAGE_NAME = "Copied image Name"
PARAM_LABEL_IMAGE_TAGS = "Image tags"
PARAM_LABEL_SOURCE_TAGS = "Source image tags"
PARAM_LABEL_MAX_INSTANCES = "Max concurrency"

PARAM_ACCOUNTS_LAUNCH_PERMISSIONS = "ExecutePermission"
PARAM_COPIED_IMAGE_TAGS = "CopiedImageTags"
PARAM_COPIED_IMAGES = "SourceImageTypes"
PARAM_COPY_SHARED_FROM_ACCOUNTS = "CopiedSharedFromAccounts"
PARAM_CROSS_ACCOUNT_TAG_ROLENAME = "TaggingSharedAccountRolename"
PARAM_DESTINATION_REGION = "DestinationRegion"
PARAM_ENCRYPTED = "Encrypted"
PARAM_KMS_KEY_ID = "KmsKeyId"
PARAM_MAX_INSTANCES = "MaxInstances"
PARAM_IMAGE_DESCRIPTION = "ImageDescription"
PARAM_IMAGE_NAME = "ImageName"
PARAM_IMAGE_TAGS = "ImageTags"
PARAM_SOURCE_TAGS = "SourceTags"

DEBUG_ONLY_COPY_OWNED_IMAGES = "Image {} is owned by account {}, because option {} is set to only copy images " \
                               "owned by account {} it is not selected"
DEBUG_ONLY_COPY_SHARED_IMAGES = "Image {} is owned by account {}, because option {} is set to only copy images " \
                                "shared to account {} it is not selected"
DEBUG_SHARED_IMAGE_OWNER_NOT_IN_LIST = "Tags {} shared by account {} is not copied as it is not in the list of accounts" \
                                       " to copy images from {}"
DEBUG_IMAGE_ALREADY_COPIED_NOT_SELECTED = "Image {} not selected as it already has been copied as image {} in destination region {}"
DEBUG_IMAGE_ALREADY_COPIED_NOT_EXECUTED = "Image {} not copied as it already has been copied as image {} in destination region {}"

INF_COMPLETED_ALREADY_COPIED = "Completed as the source image was already copied to image {} in region {}"
INF_ACCOUNT_IMAGE = "Copying image {} for account {} from region {} to region{}"
INF_CHECK_COMPLETED_RESULT = "Image copy completion check result is {}"
INF_COPY_COMPLETED = "Image {} from region {} copied to image {} in region {}"
INF_COPY_PENDING = "Image with id {} does not exist or is pending in region {}"
INF_CREATE_COPIED_TAGS = "Creating tags {} for copied image"
INF_CREATE_SOURCE_TAGS = "Creating tags {} for source image"
INF_IMAGE_COPIED = "Copy of  image {} to region {} image {} started"
INF_NO_ROLE_TO_SET_TAG = "No matching role for account {} in {} to set tags on shared image"
INF_SETTING_CREATE_IMAGE_PERMISSIONS = "Setting create execute permissions for {}"
INF_TAGS_CREATED = "Tags created for copied image"
INF_USING_OWN_ROLE_TO_SET_TAGS = "Using Ops Automator Role to set tags on shared image for account {}"
INF_USING_ROLE_TO_SET_TAGS = "Using role {} to tag shared image in account {}"
INF_CREATE_SHARED_TAGS = "Creating tags {} for shared snapshot in account {}"

ERR_ACCOUNTS_BUT_NOT_SHARED = "Parameter {} can only be used if {} parameter has been set to copy shared images"
ERR_INVALID_DESTINATION_REGION = "{} is not a valid region, valid regions are: "
ERR_INVALID_KMS_ID_ARN = "{} is not a valid KMS Id ARN"
ERR_KMS_KEY_NOT_IN_REGION = "KMS key with id {} is not available in destination region {}"
ERR_KMS_KEY_ONLY_IF_ENCRYPTED = "{} parameter can only be used if encryption is enabled"
ERR_SETTING_CREATE_VOLUME_PERMISSIONS = "Error setting create volume permissions for account(s) {}, {}"
ERR_COPY_IMAGE = "Error copying image"
ERR_TAGS_NOT_SET_IN_ACCOUNT = "Tags not set in account {}"
ERR_SETTING_SHARED_TAGS = "Can not set tags for copied shared images in account {}, {}"


class Ec2CopyImageAction(ActionBase):
    """
    Class implements action for copying EC2 Images
    """

    properties = {
        ACTION_TITLE: "EC2 Copy Image",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Copies EC2 image (AMI)",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "817df008-40fb-483b-948a-b4cb5ee67876",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.IMAGES,
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

        ACTION_SELECT_EXPRESSION:
            "Images[?State=='available' && !ImageOwnerAlias ]."
            "{ImageId:ImageId, "
            "VolumeId:VolumeId, OwnerId:OwnerId, "
            "CreationDate:CreationDate,"
            "Name:Name, "
            "Description:Description, "
            "Tags:Tags}",

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'Filters': [{"Name": "is-public", "Values": ["false"]}]},

        ACTION_MAX_CONCURRENCY: lambda parameters: min(int(parameters.get(PARAM_MAX_INSTANCES, 5)),
                                                       int(os.getenv(handlers.ENV_SERVICE_LIMIT_CONCURRENT_IMAGE_COPY,
                                                                     DEFAULT_MAX_INSTANCES))),

        ACTION_PARAMETERS: {
            PARAM_DESTINATION_REGION: {
                PARAM_DESCRIPTION: PARAM_DESC_DESTINATION_REGION,
                PARAM_LABEL: PARAM_LABEL_DESTINATION_REGION,
                PARAM_TYPE: str,
                PARAM_DEFAULT: services.get_session().region_name,
                PARAM_ALLOWED_VALUES: [str(r) for r in services.get_session().get_available_regions("ec2", "aws-us-gov")]
            },
            PARAM_IMAGE_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_IMAGE_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_IMAGE_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_IMAGE_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_IMAGE_NAME,
                PARAM_LABEL: PARAM_LABEL_IMAGE_NAME,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_COPIED_IMAGE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_IMAGE_TAGS,
                PARAM_LABEL: PARAM_LABEL_COPIED_IMAGE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_IMAGE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_IMAGE_TAGS,
                PARAM_LABEL: PARAM_LABEL_IMAGE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_COPIED_IMAGES: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_IMAGES,
                PARAM_LABEL: PARAM_LABEL_COPIED_IMAGES,
                PARAM_TYPE: str,
                PARAM_ALLOWED_VALUES: [COPIED_OWNED_BY_ACCOUNT, COPIED_IMAGES_SHARED_TO_ACCOUNT, COPIED_IMAGES_BOTH],
                PARAM_DEFAULT: COPIED_OWNED_BY_ACCOUNT
            },
            PARAM_SOURCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_TAGS,
                PARAM_LABEL: PARAM_LABEL_SOURCE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_ENCRYPTED: {
                PARAM_DESCRIPTION: PARAM_DESC_ENCRYPTED,
                PARAM_LABEL: PARAM_LABEL_ENCRYPTED,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False
            },
            PARAM_MAX_INSTANCES: {
                PARAM_DESCRIPTION: PARAM_DESC_MAX_INSTANCES.format(1, os.getenv(handlers.ENV_SERVICE_LIMIT_CONCURRENT_IMAGE_COPY,
                                                                                DEFAULT_MAX_INSTANCES)),
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 5,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: int(os.getenv(handlers.ENV_SERVICE_LIMIT_CONCURRENT_IMAGE_COPY, DEFAULT_MAX_INSTANCES)),
                PARAM_LABEL: PARAM_LABEL_MAX_INSTANCES
            },
            PARAM_ACCOUNTS_LAUNCH_PERMISSIONS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_EXECUTE_PERMISSIONS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_EXECUTE_PERMISSIONS
            },
            PARAM_CROSS_ACCOUNT_TAG_ROLENAME: {
                PARAM_DESCRIPTION: PARAM_DESC_CROSS_ACCOUNT_TAG_ROLENAME,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_CROSS_ACCOUNT_TAG_ROLENAME
            },
            PARAM_COPY_SHARED_FROM_ACCOUNTS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPY_SHARED_FROM_ACCOUNTS,
                PARAM_LABEL: PARAM_LABEL_COPY_SHARED_FROM_ACCOUNTS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False
            },
            PARAM_KMS_KEY_ID: {
                PARAM_DESCRIPTION: PARAM_DESC_KMS_KEY_ID,
                PARAM_LABEL: PARAM_LABEL_KMS_KEY_ID,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_IMAGE_COPY_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_DESTINATION_REGION,
                    PARAM_COPIED_IMAGES,
                    PARAM_COPY_SHARED_FROM_ACCOUNTS,
                    PARAM_COPIED_IMAGE_TAGS,
                    PARAM_IMAGE_TAGS,
                    PARAM_IMAGE_NAME,
                    PARAM_IMAGE_DESCRIPTION,
                    PARAM_SOURCE_TAGS,
                    PARAM_CROSS_ACCOUNT_TAG_ROLENAME
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ENCRYPTED,
                    PARAM_KMS_KEY_ID,
                    PARAM_ACCOUNTS_LAUNCH_PERMISSIONS,
                    PARAM_MAX_INSTANCES
                ],
            }
        ],

        ACTION_PERMISSIONS: ["ec2:CopyImage",
                             "ec2:CreateTags",
                             "ec2:DeleteTags",
                             "ec2:DescribeImages",
                             "ec2:ModifyImageAttribute"]

    }

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        # source image
        image_id = resource["ImageId"]

        # owner of the image
        image_owner = resource["OwnerId"]

        parameters = task.get("parameters", {})

        # copy owned, shared or both
        copied_images = parameters[PARAM_COPIED_IMAGES]

        account = resource["AwsAccount"]

        if copied_images == COPIED_OWNED_BY_ACCOUNT and account != image_owner:
            logger.debug(DEBUG_ONLY_COPY_OWNED_IMAGES, image_id, image_owner, PARAM_COPIED_IMAGES, account)
            return None

        if copied_images == COPIED_IMAGES_SHARED_TO_ACCOUNT and account == image_owner:
            logger.debug(DEBUG_ONLY_COPY_SHARED_IMAGES, image_id, image_owner, PARAM_COPIED_IMAGES, account)
            return None

        copy_from_accounts = parameters.get(PARAM_COPY_SHARED_FROM_ACCOUNTS, None)
        if copy_from_accounts not in [None, []]:

            if copied_images == COPIED_OWNED_BY_ACCOUNT:
                raise_value_error(ERR_ACCOUNTS_BUT_NOT_SHARED, PARAM_COPY_SHARED_FROM_ACCOUNTS, PARAM_COPIED_IMAGES)
            if image_owner != account and image_owner not in [a.strip() for a in copy_from_accounts]:
                logger.debug(DEBUG_SHARED_IMAGE_OWNER_NOT_IN_LIST, image_id, image_owner, ",".join(copy_from_accounts))
                return None

        copied_tag_name = Ec2CopyImageAction.marker_tag_copied_to(task[handlers.TASK_NAME])
        resource_tags = resource.get("Tags", {})
        if copied_tag_name not in resource_tags:
            resource["SourceInstanceId"] = resource_tags.get(marker_image_source_instance_tag(), "")
            return resource

        copied_image_data = json.loads(resource["Tags"][copied_tag_name])
        logger.debug(DEBUG_IMAGE_ALREADY_COPIED_NOT_SELECTED, image_id, copied_image_data.get("image-id", ""),
                     copied_image_data.get("region", ""))
        return None

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        valid_regions = services.get_session().get_available_regions("ec2", "aws-us-gov")
        region = parameters.get(PARAM_DESTINATION_REGION)
        if region not in valid_regions:
            raise_value_error(ERR_INVALID_DESTINATION_REGION, region, ",".join(valid_regions))

        kms_key_id = parameters.get(PARAM_KMS_KEY_ID, None)
        if not parameters[PARAM_ENCRYPTED] and kms_key_id not in ["", None]:
            raise_value_error(ERR_KMS_KEY_ONLY_IF_ENCRYPTED, PARAM_KMS_KEY_ID)

        if kms_key_id not in ["", None]:
            if re.match(KMS_KEY_ID_PATTERN, kms_key_id) is None:
                raise_value_error(ERR_INVALID_KMS_ID_ARN, kms_key_id)

            destination_region = parameters[PARAM_DESTINATION_REGION]
            if kms_key_id.split(":")[3] != destination_region:
                raise_value_error(ERR_KMS_KEY_NOT_IN_REGION, kms_key_id, destination_region)

        return parameters

    @staticmethod
    def marker_tag_copied_to(task):
        return MARKER_TAG_COPIED_TO_TEMPLATE.format(os.getenv(handlers.ENV_STACK_NAME)).format(task)

    @staticmethod
    def action_concurrency_key(parameters):
        """
        Returns key for concurrency control of the scheduler.
        :return: Concurrency key
        """
        return "ec2:CopyImage-{}-{}".format(parameters[ACTION_PARAM_TASK], parameters[ACTION_PARAM_ACCOUNT])

    @staticmethod
    def marker_tag_source_image_id():
        return MARKER_TAG_SOURCE_IMAGE_ID_TEMPLATE.format(os.getenv(handlers.ENV_STACK_NAME))

    @staticmethod
    def action_logging_subject(arguments, _):
        image = arguments[ACTION_PARAM_RESOURCES]
        account = image["AwsAccount"]
        image_id = image["ImageId"]
        region = image["Region"]
        return "{}-{}-{}-{}".format(account, region, image_id, log_stream_date())

    @property
    def ec2_destination_client(self):

        if self._ec2_destination_client is None:
            methods = ["copy_image",
                       "create_tags",
                       "delete_tags",
                       "modify_image_attribute"]

            self._ec2_destination_client = get_client_with_retries("ec2",
                                                                   methods=methods,
                                                                   region=self.destination_region,
                                                                   context=self._context_,
                                                                   session=self._session_,
                                                                   logger=self._logger_)
        return self._ec2_destination_client

    @property
    def ec2_source_client(self):
        if self._ec2_source_client is None:
            methods = ["create_tags",
                       "delete_tags"]

            self._ec2_source_client = get_client_with_retries("ec2",
                                                              methods=methods,
                                                              region=self.source_region,
                                                              context=self._context_,
                                                              session=self._session_,
                                                              logger=self._logger_)
        return self._ec2_source_client

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        # debug and dryrun
        self.source_image = self._resources_
        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        # image source and destination information
        self.source_image_id = self.source_image["ImageId"]
        self.source_region = self.source_image["Region"]
        self.destination_region = self.get(PARAM_DESTINATION_REGION)

        self.encrypted = self.get(PARAM_ENCRYPTED, False)
        self.kms_key_id = self.get(PARAM_KMS_KEY_ID, None)
        self.copied_images = self.get(PARAM_COPIED_IMAGES)
        self.accounts_with_launch_permissions = self.get(PARAM_ACCOUNTS_LAUNCH_PERMISSIONS, [])
        self.cross_account_shared_image_tagging = self.get(PARAM_CROSS_ACCOUNT_TAG_ROLENAME, [])
        # tagging
        self.copied_image_tagfiter = TagFilterSet(self.get(PARAM_COPIED_IMAGE_TAGS, ""))

        self.source_instance_id = self.source_image["SourceInstanceId"]

        self._ec2_destination_client = None
        self._ec2_source_client = None

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "destination-region": self.destination_region,
            "source-region": self.source_region,
            "source-image-id": self.source_image_id,
            "encrypted": self.encrypted,
            "kms-id": self.kms_key_id if self.kms_key_id is not None else ""
        }

    def is_completed(self, image_copy_execute_data):

        def set_source_image_tags(copy_img_id, copy_image_name):
            image_tags = {}
            image_tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SOURCE_TAGS,
                                              region=self.source_region,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_COPIED_IMAGE_ID: copy_img_id,
                                                  TAG_PLACEHOLDER_COPIED_REGION: self.destination_region,
                                                  TAG_PLACEHOLDER_COPIED_NAME: copy_image_name
                                              }))

            self._logger_.info(INF_CREATE_SOURCE_TAGS, image_tags)

            if len(image_tags) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_source_client,
                                     resource_ids=[self.source_image_id],
                                     tags=image_tags,
                                     logger=self._logger_)

                self._logger_.info(INF_TAGS_CREATED)

        def grant_launch_image_permissions(img_id):

            if self.accounts_with_launch_permissions is not None and len(self.accounts_with_launch_permissions) > 0:

                args = {

                    "Attribute": "LaunchPermission",
                    "LaunchPermission": {
                        "Add": [{"UserId": a.strip()} for a in self.accounts_with_launch_permissions]

                    },
                    "ImageId": img_id,

                    "OperationType": "add"
                }

                try:
                    self.ec2_destination_client.modify_image_attribute_with_retries(**args)
                    self._logger_.info(INF_SETTING_CREATE_IMAGE_PERMISSIONS, ", ".join(self.accounts_with_launch_permissions))
                except Exception as ex_perm:
                    raise_exception(ERR_SETTING_CREATE_VOLUME_PERMISSIONS, self.accounts_with_launch_permissions, ex_perm)

        def tag_shared_image(tags, img_id):

            if len(tags) == 0:
                return

            if self.accounts_with_launch_permissions is None:
                self.accounts_with_launch_permissions = []

            for account in self.accounts_with_launch_permissions:
                session_for_tagging = self.get_action_session(account=account,
                                                              param_name=PARAM_CROSS_ACCOUNT_TAG_ROLENAME,
                                                              logger=self._logger_)

                if session_for_tagging is None:
                    self._logger_.error(ERR_TAGS_NOT_SET_IN_ACCOUNT, account)
                    continue

                try:
                    ec2_client = get_client_with_retries(service_name="ec2",
                                                         methods=[
                                                             "create_tags",
                                                             "delete_tags"
                                                         ],
                                                         context=self._context_,
                                                         region=self.get(PARAM_DESTINATION_REGION),
                                                         session=session_for_tagging,
                                                         logger=self._logger_)

                    tagging.set_ec2_tags(ec2_client=ec2_client,
                                         resource_ids=[img_id],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INF_CREATE_SHARED_TAGS, tags, account)

                except Exception as ex_sharing:
                    raise_exception(ERR_SETTING_SHARED_TAGS, account, str(ex_sharing))

        already_copied_data = image_copy_execute_data.get("already-copied", None)
        if already_copied_data is not None:
            self._logger_.info(INF_COMPLETED_ALREADY_COPIED, already_copied_data.get("image-id", ""),
                               already_copied_data.get("region", ""))
            return image_copy_execute_data

        # create service instance to test if image exists
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))
        copy_image_id = image_copy_execute_data["copy-image-id"]

        # test if the image with the id that was returned from the CopyImage API call exists and is completed
        try:
            copied_image = ec2.get(services.ec2_service.IMAGES,
                                   region=self.destination_region,
                                   Owners=["self"],
                                   ImageIds=[copy_image_id])
        except Exception as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code", "") == "InvalidAMIID.NotFound":
                copied_image = None
            else:
                raise ex

        if copied_image is not None:
            self._logger_.debug(INF_CHECK_COMPLETED_RESULT, copied_image)

        state = copied_image["State"] if copied_image is not None else None

        if copied_image is None or state == IMAGE_STATE_PENDING:
            self._logger_.info(INF_COPY_PENDING, copy_image_id, self.destination_region)
            return None

        if state == IMAGE_STATE_FAILED:
            copied_tag_name = Ec2CopyImageAction.marker_tag_copied_to(self._task_[handlers.TASK_NAME])
            self.ec2_source_client.delete_tags_with_retries(Resources=[self.source_image_id], Tags=[{"Key": copied_tag_name}])
            raise_exception(ERR_COPY_IMAGE)

        if state == IMAGE_STATE_COMPLETED:
            self._logger_.info(INF_COPY_COMPLETED, self.source_image_id, self.source_region, copy_image_id,
                               self.destination_region)
            grant_launch_image_permissions(copy_image_id)
            tag_shared_image(image_copy_execute_data.get("tags", {}), copy_image_id)
            set_source_image_tags(copy_image_id, copied_image.get("Name", ""))
            return copied_image

        return None

    def execute(self):
        def get_tags_for_copied_image():

            image_tags = (self.copied_image_tagfiter.pairs_matching_any_filter(self.source_image.get("Tags", {})))

            if self.source_instance_id not in [None, ""]:
                image_tags[marker_image_source_instance_tag()] = self.source_instance_id

            image_tags.update(
                self.build_tags_from_template(parameter_name=PARAM_IMAGE_TAGS,
                                              region=self.source_region,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_SOURCE_IMAGE_ID: self.source_image_id,
                                                  TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                  TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_,
                                                  TAG_PLACEHOLDER_SOURCE_NAME: self.source_image.get("Name", ""),
                                                  TAG_PLACEHOLDER_SOURCE_INSTANCE: self.source_instance_id,
                                                  TAG_PLACEHOLDER_SOURCE_DESCRIPTION: self.source_image.get("Description", "")

                                              }))

            image_tags[Ec2CopyImageAction.marker_tag_source_image_id()] = self.source_image_id

            return image_tags

        # logged information
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])
        self._logger_.info(INF_ACCOUNT_IMAGE, self.source_image_id, self._account_, self.source_region, self.destination_region)
        self._logger_.debug("Image : {}", self.source_image)

        copied_tag_name = Ec2CopyImageAction.marker_tag_copied_to(self._task_)
        resource_tags = self.source_image.get("Tags", {})
        if copied_tag_name in resource_tags:
            copied_image_data = json.loads(self.source_image["Tags"][copied_tag_name])
            self._logger_.info(DEBUG_IMAGE_ALREADY_COPIED_NOT_EXECUTED, self.source_image_id, copied_image_data.get("image-id", ""),
                               copied_image_data.get("region", ""))
            self.result["already-copied"] = copied_image_data
            return self.result

        boto_call = "copy_image"
        try:

            # setup argument for CopyImage call
            args = {
                "SourceRegion": self.source_region,
                "SourceImageId": self.source_image_id
            }

            if self.encrypted:
                args["Encrypted"] = True
                self.result["encrypted"] = True
                if self.kms_key_id not in ["", None]:
                    args["KmsKeyId"] = self.kms_key_id

            if self.dryrun:
                args["DryRun"] = True

            args["Description"] = self.build_str_from_template(parameter_name=PARAM_IMAGE_DESCRIPTION,
                                                               region=self.source_region,
                                                               tag_variables={
                                                                   TAG_PLACEHOLDER_SOURCE_DESCRIPTION: self.source_image.get(
                                                                       "Description", ""),
                                                                   TAG_PLACEHOLDER_SOURCE_NAME: self.source_image.get("Name", ""),
                                                                   TAG_PLACEHOLDER_SOURCE_IMAGE_ID: self.source_image_id,
                                                                   TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                                   TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_,
                                                                   TAG_PLACEHOLDER_SOURCE_INSTANCE: self.source_instance_id
                                                               })
            if args["Description"] == "":
                args["Description"] = self.source_image.get("Description", "")

            args["Name"] = self.build_str_from_template(parameter_name=PARAM_IMAGE_NAME,

                                                        region=self.source_region,

                                                        tag_variables={
                                                            TAG_PLACEHOLDER_SOURCE_NAME: self.source_image.get("Name", ""),
                                                            TAG_PLACEHOLDER_SOURCE_IMAGE_ID: self.source_image_id,
                                                            TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                            TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_,
                                                            TAG_PLACEHOLDER_SOURCE_INSTANCE: self.source_instance_id
                                                        })
            if args["Name"] == "":
                args["Name"] = self.source_image.get("Name", "")

            # start the copy
            resp = self.ec2_destination_client.copy_image_with_retries(**args)

            # id of the copy
            copy_image_id = resp.get("ImageId")
            self._logger_.info(INF_IMAGE_COPIED, self.source_image_id, self.destination_region, copy_image_id)
            self.result[boto_call] = resp
            self.result["copy-image-id"] = copy_image_id
            self.result["copy-image-name"] = args["Name"]

            # set tag on the source to avoid multiple copies
            boto_call = "create_tags (source)"
            copied_tag_name = Ec2CopyImageAction.marker_tag_copied_to(self._task_)

            boto_call = "create_tags (source)"
            self.ec2_source_client.create_tags_with_retries(Resources=[self.source_image_id],
                                                            Tags=tag_key_value_list({
                                                                copied_tag_name: safe_json(
                                                                    {
                                                                        "region": self.destination_region,
                                                                        "image-id": copy_image_id
                                                                    })
                                                            }))

            # set tags on the copy
            boto_call = "create_tags (target)"
            tags = get_tags_for_copied_image()

            if len(tags) > 0:
                self._logger_.info(INF_CREATE_COPIED_TAGS, tags)
                tagging.set_ec2_tags(ec2_client=self.ec2_destination_client,
                                     resource_ids=[copy_image_id],
                                     tags=tags,
                                     can_delete=False,
                                     logger=self._logger_)

                self.result["tags"] = tags
                self._logger_.info(INF_TAGS_CREATED)

        except Exception as ex:
            if self.dryrun:
                self._logger_.debug(str(ex))
                self.result[boto_call] = str(ex)
                return self.result
            else:
                raise ex

        self.result[METRICS_DATA] = build_action_metrics(self, CopiedImages=1)

        return self.result
