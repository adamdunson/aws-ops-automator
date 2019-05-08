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
import tagging
from actions import *
from actions.action_base import ActionBase
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_IMAGE_NAME = "image-name"
TAG_PLACEHOLDER_IMAGE_ID = "image-id"
TAG_PLACEHOLDER_INSTANCE = "instance-id"

ERR_CREATING_IMAGE_START = "Error creating image for instance {}, {}"
ERR_CREATING_IMAGE = "Creation of image for instance {} failed, reason is {}"
ERR_SETTING_IMAGE_TAGS = "Error setting tags to image {}, {}"
ERR_SETTING_INSTANCE_TAGS = "Error setting tags to instance {}, {}"
ERR_SETTING_LAUNCH_PERMISSIONS = "Error setting launch permissions for accounts {}, {}"

IMAGE_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"
IMAGE_DESCRIPTION = "Image created by task {} for instance {}"

INF_CREATE_IMAGE = "Creation of image {} for instance {} started"
INF_IMAGE_DATA = "Image data is {}"
INF_IMAGE_NOT_CREATED_YET = "Image {} does has not been created yet"
INF_IMAGE_STATUS = "Image status is {}"
INF_NOT_COMPLETED = "Image not completed yet"
INF_SET_INSTANCE_TAGS = "Set tags {} to instance {}"
INF_SETTING_IMAGE_TAGS = "Set tags {} to image {}"
INF_START_CHECK = "Create start result data is {}"
INF_START_IMAGE_CREATE_ACTION = "Creating image for EC2 instance {} for task {}"
INF_STATUS_IMAGE = "Checking status of image {}"
INF_SETTING_LAUNCH_PERMISSIONS = "Launch access granted to accounts"

GROUP_TITLE_TAGGING_NAMING = "Image tagging and naming options"
GROUP_TITLE_IMAGE_OPTIONS = "Image creation options"

PARAM_AMI_NAME_PREFIX = "ImageNamePrefix"
PARAM_AMI_TAGS = "ImageTags"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_MAX_CONCURRENT = "MaxConcurrent"
PARAM_NO_REBOOT = "NoReboot"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_ACCOUNTS_LAUNCH_ACCESS = "AccountsLaunchAccess"
PARAM_IMAGE_DESCRIPTION = "ImageDescription"
PARAM_NAME = "ImageName"

PARAM_DESC_AMI_NAME_PREFIX = "Prefix for image name."
PARAM_DESC_AMI_TAGS = "Tags that will be added to created image. Use a list of tagname=tagvalue pairs."
PARAM_DESC_INSTANCE_TAGS = "Tags to set on source EC2 instance after the images has been created successfully."
PARAM_DESC_ACCOUNTS_LAUNCH_ACCESS = "List of valid AWS account ID that will be granted launch permissions for the image."
PARAM_DESC_COPIED_INSTANCE_TAGS = "Enter a tag filter to copy tags from the instance to the AMI.\
                                   For example, enter * to copy all tags from the instance to the image."
PARAM_DESC_NO_REBOOT = "When enabled, Amazon EC2 does not shut down the instance before creating the image. " \
                       "When this option is used, file system integrity on the created image cannot be guaranteed."
PARAM_DESC_IMAGE_DESCRIPTION = "Description of created image."
PARAM_DESC_NAME = "Name of the created image, leave blank for default image name"
PARAM_DESC_MAX_CONCURRENT = "Maximum number of concurrent image creation tasks running concurrently per account (1-50)"

PARAM_LABEL_AMI_NAME_PREFIX = "Image name prefix"
PARAM_LABEL_AMI_TAGS = "Image tags"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied instance tags"
PARAM_LABEL_MAX_CONCURRENT = "Concurrent image tasks"
PARAM_LABEL_NO_REBOOT = "No reboot"
PARAM_LABEL_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_ACCOUNTS_LAUNCH_ACCESS = "Accounts with launch access"
PARAM_LABEL_IMAGE_DESCRIPTION = "Image description"
PARAM_LABEL_NAME = "Image name"


class Ec2CreateImageAction(ActionEc2EventBase):
    """
    Implements action to create image for an EC2 instance
    """
    properties = {
        ACTION_TITLE: "EC2 Create AMI",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPTION: "Creates Amazon Machine Image (AMI) for an EC2 Instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6d347b08-94ec-4c26-ba74-eeae7d9e9921",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
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

        ACTION_SELECT_EXPRESSION: "Reservations[*].Instances[].{InstanceId:InstanceId, Tags:Tags, State:State.Name}"
                                  "|[?State!='terminated']",

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION:
                    [handlers.ec2_state_event_handler.EC2_STATE_RUNNING,
                     handlers.ec2_state_event_handler.EC2_STATE_STOPPED]
            },
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_AMI_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_AMI_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_AMI_TAGS
            },
            PARAM_AMI_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_AMI_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_AMI_NAME_PREFIX
            },
            PARAM_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_NAME
            },
            PARAM_IMAGE_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_IMAGE_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_IMAGE_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
            },
            PARAM_NO_REBOOT: {
                PARAM_DESCRIPTION: PARAM_DESC_NO_REBOOT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_NO_REBOOT
            },
            PARAM_ACCOUNTS_LAUNCH_ACCESS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_LAUNCH_ACCESS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_LAUNCH_ACCESS
            },
            PARAM_MAX_CONCURRENT: {
                PARAM_DESCRIPTION: PARAM_DESC_MAX_CONCURRENT,
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 50,
                PARAM_LABEL: PARAM_LABEL_MAX_CONCURRENT,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: 50
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_IMAGE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_NO_REBOOT,
                    PARAM_ACCOUNTS_LAUNCH_ACCESS,
                    PARAM_INSTANCE_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_AMI_TAGS,
                    PARAM_AMI_NAME_PREFIX,
                    PARAM_NAME,
                    PARAM_IMAGE_DESCRIPTION
                ],
            }],

        ACTION_PERMISSIONS: ["ec2:CreateImage",
                             "ec2:DescribeTags",
                             "ec2:DescribeInstances",
                             "ec2:CreateTags",
                             "ec2:DeleteTags",
                             "ec2:DescribeImages",
                             "ec2:ModifyImageAttribute"],

    }

    @staticmethod
    def action_logging_subject(arguments, _):
        instance = arguments[ACTION_PARAM_RESOURCES]
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}-{}".format(account, region, instance_id, log_stream_date())

    @staticmethod
    def action_concurrency_key(arguments):
        return "ec2:CreateImage:{}:{}".format(arguments[ACTION_PARAM_ACCOUNT], arguments[PARAM_MAX_CONCURRENT])

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = ["create_image",
                       "describe_tags",
                       "describe_instances",
                       "create_tags",
                       "delete_tags",
                       "describe_images",
                       "modify_image_attribute"]

            self._ec2_client = get_client_with_retries("ec2", methods, region=self.instance["Region"],
                                                       session=self._session_, logger=self._logger_)

        return self._ec2_client

    def _create_instance_tags(self, image_id, image_name):

        tags = self.build_tags_from_template(PARAM_INSTANCE_TAGS,
                                             tag_variables={
                                                 TAG_PLACEHOLDER_IMAGE_ID: image_id,
                                                 TAG_PLACEHOLDER_IMAGE_NAME: image_name
                                             })

        if len(tags) > 0:
            try:

                self.set_ec2_instance_tags_with_event_loop_check(instance_ids=[self.instance_id],
                                                                 tags_to_set=tags,
                                                                 client=self.ec2_client,
                                                                 region=self._region_)

                self._logger_.info(INF_SET_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]), self.instance_id)
            except Exception as ex:
                raise_exception(ERR_SETTING_INSTANCE_TAGS, self.instance_id, ex)

    def _grant_launch_access(self, image_id):

        if self.accounts_with_launch_access is not None and len(self.accounts_with_launch_access) > 0:
            args = {
                "ImageId": image_id,
                "LaunchPermission": {
                    "Add": [{"UserId": a.strip()} for a in self.accounts_with_launch_access]
                }
            }

            try:
                self.ec2_client.modify_image_attribute_with_retries(**args)
                self._logger_.info(INF_SETTING_LAUNCH_PERMISSIONS, ", ".join(self.accounts_with_launch_access))
                self.result["launch-access-accounts"] = [a.strip() for a in self.accounts_with_launch_access]
            except Exception as ex:
                raise_exception(ERR_SETTING_LAUNCH_PERMISSIONS, self.accounts_with_launch_access, ex)

    def _create_image(self):

        image_name = self.build_str_from_template(parameter_name=PARAM_NAME,
                                                  tag_variables={
                                                      TAG_PLACEHOLDER_INSTANCE: self.instance_id
                                                  })

        if image_name == "":
            dt = self._datetime_.utcnow()
            image_name = IMAGE_NAME.format(self.instance_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

        prefix = self.build_str_from_template(parameter_name=PARAM_AMI_NAME_PREFIX,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_INSTANCE: self.instance_id
                                              })
        image_name = prefix + image_name

        description = self.build_str_from_template(parameter_name=PARAM_IMAGE_DESCRIPTION,
                                                   tag_variables={
                                                       TAG_PLACEHOLDER_INSTANCE: self.instance_id
                                                   })
        args = {
            "InstanceId": self.instance_id,
            "NoReboot": self.no_reboot,
            "Name": image_name,
            "Description": description
        }

        try:
            self._logger_.debug("create_image arguments {}", args)
            resp = self.ec2_client.create_image_with_retries(**args)
            image_id = resp.get("ImageId", None)
            self._logger_.info(INF_CREATE_IMAGE, image_id, self.instance_id)
            self._create_image_tags(image_id)
            return image_id
        except Exception as ex:
            raise_exception(ERR_CREATING_IMAGE_START, self.instance_id, ex)

    def _create_image_tags(self, image_id):

        tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.instance_tags)

        tags.update(
            self.build_tags_from_template(parameter_name=PARAM_AMI_TAGS,
                                          tag_variables={TAG_PLACEHOLDER_INSTANCE: self.instance_id}))

        tags[marker_image_source_instance_tag()] = self.instance_id

        if len(tags) > 0:
            try:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=[image_id],
                                     tags=tags,
                                     can_delete=False,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_IMAGE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   image_id)
            except Exception as ex:
                raise_exception(ERR_SETTING_IMAGE_TAGS, image_id, ex)

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_
        self.instance_id = self.instance["InstanceId"]

        self.no_reboot = self.get(PARAM_NO_REBOOT, True)

        # tags from the Ec2 instance
        self.instance_tags = self.instance.get("Tags", {})
        # filter for tags copied from ec2 instance to image
        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.accounts_with_launch_access = self.get(PARAM_ACCOUNTS_LAUNCH_ACCESS, [])

        self._ec2_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
            "task": self._task_
        }

    def is_completed(self, image_create_data):

        self._logger_.debug(INF_START_CHECK, safe_json(image_create_data))

        image_id = image_create_data["image-id"]
        self._logger_.debug(safe_json(INF_STATUS_IMAGE, indent=3))

        # create service instance to test is image is available
        strategy = get_default_retry_strategy("ec2", context=self._context_)
        ec2 = services.create_service("ec2", session=self._session_, service_retry_strategy=strategy)

        # get image information
        try:
            image = ec2.get(services.ec2_service.IMAGES,
                            Owners=["self"],
                            ImageIds=[image_id],
                            tags=True,
                            region=self.instance["Region"])
        except Exception as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code", "") == "InvalidAMIID.NotFound":
                image = None
            else:
                raise ex

        if image is None:
            self._logger_.info(INF_IMAGE_NOT_CREATED_YET, image_id)
            return None
        self._logger_.debug(INF_IMAGE_DATA, safe_json(image, indent=3))

        # get and test image state
        status = image["State"]
        self._logger_.info(INF_IMAGE_STATUS, status)

        if status == "pending":
            self._logger_.info(INF_NOT_COMPLETED)
            return None

        # abort if creation is not successful
        if status in ["failed", "error", "invalid"]:
            raise Exception(
                ERR_CREATING_IMAGE.format(self.instance_id, image.get("StateReason", {}).get("Message", "")))

        # image creation is done
        if status == "available":
            self.result["name"] = image["Name"]
            self.result["image-id"] = image["ImageId"]
            self.result["creation-date"] = image["CreationDate"]

            # tag created image
            self._create_instance_tags(image_id, image["Name"])

            # grant launch access to accounts
            self._grant_launch_access(image_id)

            return self.result

        return None

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_START_IMAGE_CREATE_ACTION, self.instance_id, self._task_)

        image_id = self._create_image()
        self.result["image-id"] = image_id

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CreatedImages=1
        )

        return self.result
