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


import handlers.ebs_snapshot_event_handler
import handlers.ec2_state_event_handler
import handlers.rds_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_value_error

ERR_BAD_RESOURCE_TYPE = "Action can only create tags for EC2 resources types {}"

GROUP_LABEL_TAG_OPTIONS = "Resource Tags"
PARAM_DESC_RESOURCE_TAGS = "Tags to add to resource as a comma delimited list of name=value pairs."
PARAM_LABEL_RESOURCE_TAGS = "Tags"
PARAM_RESOURCE_TAGS = "Tags"

INF_SET_TAGS = "Set tags {} to resources {}, task is {}"

ALLOWED_RESOURCES = [
    services.ec2_service.INSTANCES,
    services.ec2_service.SNAPSHOTS,
    services.ec2_service.VOLUMES,
    services.ec2_service.IMAGES
]


class Ec2SetTagsAction(ActionBase):
    """
    Class implements tagging of selected EC2 resources
    """

    properties = {
        ACTION_TITLE: "EC2 Set Tags",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Set tags for EC2 Instances, EBS Snapshots, Volumes and Images",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "8cc8e7f2-080d-455a-9ff1-ee6f7fc6598e",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_NOTIFICATION: [
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_CREATED,
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_COPIED,
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_SHARED],
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: [
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING,
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED,
                    handlers.ec2_state_event_handler.EC2_STATE_TERMINATED]
            },
            handlers.RDS_EVENT_SOURCE: {
                handlers.rds_event_handler.RDS_AWS_API_CALL: [
                    handlers.rds_event_handler.RDS_INSTANCE_STARTED,
                    handlers.rds_event_handler.RDS_INSTANCE_STOPPED,
                    handlers.rds_event_handler.RDS_CLUSTER_STARTED,
                    handlers.rds_event_handler.RDS_CLUSTER_STOPPED
                ]
            }
        },

        ACTION_EVENT_SCOPES: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: {
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING: handlers.EVENT_SCOPE_REGION,
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED: handlers.EVENT_SCOPE_REGION,
                    handlers.ec2_state_event_handler.EC2_STATE_TERMINATED: handlers.EVENT_SCOPE_REGION,
                },
            },
            handlers.RDS_EVENT_SOURCE: {
                handlers.rds_event_handler.RDS_AWS_API_CALL: {
                    handlers.rds_event_handler.RDS_CLUSTER_STARTED: handlers.EVENT_SCOPE_REGION,
                    handlers.rds_event_handler.RDS_CLUSTER_STOPPED: handlers.EVENT_SCOPE_REGION,
                    handlers.rds_event_handler.RDS_INSTANCE_STARTED: handlers.EVENT_SCOPE_REGION,
                    handlers.rds_event_handler.RDS_INSTANCE_STOPPED: handlers.EVENT_SCOPE_REGION

                },
            }
        },

        ACTION_PARAM_EVENT_SOURCE_TAG_FILTER: True,

        ACTION_TRIGGERS: ACTION_TRIGGER_EVENTS,

        ACTION_PARAMETERS: {

            PARAM_RESOURCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_RESOURCE_TAGS,
                PARAM_LABEL: PARAM_LABEL_RESOURCE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True
            },
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_TAG_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_RESOURCE_TAGS
                ],
            }
        ],

        ACTION_PERMISSIONS: [
            "ec2:CreateTags",
            "ec2:DeleteTags",
            "ec2:DescribeImages",
            "ec2:DescribeInstances",
            "ec2:DescribeSnapshots",
            "ec2:DescribeVolumes",
            "rds:ListTagsForResource",
            "tag:GetResources"
        ]

    }

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=[
                                                           "create_tags",
                                                           "delete_tags"
                                                       ],
                                                       region=self._region_,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @staticmethod
    def can_execute(resources, _):
        if not all(r["ResourceTypeName"] in ALLOWED_RESOURCES for r in resources):
            raise_value_error(ERR_BAD_RESOURCE_TYPE, ",".join(ALLOWED_RESOURCES))

    def __init__(self, arguments, action_parameters):

        ActionBase.__init__(self, arguments, action_parameters)

        identifier_name = self._resources_[0]["ResourceTypeName"][0:-1] + "Id"

        self.resource_ids = [res[identifier_name] for res in self._resources_ if identifier_name in res]

        self._ec2_client = None

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "resources": self.resource_ids,
            "source": self._region_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        identifier_name = arguments[ACTION_PARAM_RESOURCES][0]["ResourceTypeName"][0:-1] + "Id"
        resource_ids = list(set([res[identifier_name] for res in arguments[ACTION_PARAM_RESOURCES] if identifier_name in res]))
        if len(resource_ids) == 1:
            return "{}-{}-{}-{}".format(account, region, resource_ids[0], log_stream_datetime())
        return "{}-{}-{}-{}".format(account, region, arguments[ACTION_ID], log_stream_datetime())

    def execute(self):
        self._logger_.info("{}, version {}, task {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION],
                           self.get(ACTION_PARAM_TASK_ID))

        tags = self.build_tags_from_template(parameter_name=PARAM_RESOURCE_TAGS)

        if len(tags) > 0:
            tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                 resource_ids=self.resource_ids,
                                 tags=tags,
                                 logger=self._logger_)

        self._logger_.info(INF_SET_TAGS,
                           tags, ','.join(self.resource_ids),
                           self.get(ACTION_PARAM_TASK_ID))

        self.result[METRICS_DATA] = build_action_metrics(self, Resources=len(self.resource_ids), Tags=len(tags))

        return self.result
