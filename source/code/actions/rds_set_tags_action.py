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
import handlers.rds_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_value_error

ERR_BAD_RESOURCE_TYPE = "Action can only create tags for  resources types of RDS types {}"

GROUP_LABEL_TAG_OPTIONS = "Resource Tags"
PARAM_DESC_RESOURCE_TAGS = "Tags to add to resource as a comma delimited list of name=value pairs."
PARAM_LABEL_RESOURCE_TAGS = "Tags"
PARAM_RESOURCE_TAGS = "Tags"

RESOURCE_ARNS = [
    "DBInstanceArn",
    "DBSnapshotArn"
]


class RdsSetTagsAction(ActionBase):
    """
    Class implements tagging of selected RDS resources
    """

    properties = {
        ACTION_TITLE: "RDS Set Tags",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Creates, updates or deletes tags for RDS Resources",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "03668753-3e7c-4aef-8ea9-531148b2fad2",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_EVENTS: {
            handlers.RDS_EVENT_SOURCE: {
                handlers.rds_event_handler.RDS_AWS_API_CALL: [
                    handlers.rds_event_handler.RDS_INSTANCE_STARTED,
                    handlers.rds_event_handler.RDS_INSTANCE_STOPPED,
                    handlers.rds_event_handler.RDS_INSTANCE_RESTORED,
                    handlers.rds_event_handler.RDS_INSTANCE_CREATED,
                    handlers.rds_event_handler.RDS_CLUSTER_STARTED,
                    handlers.rds_event_handler.RDS_CLUSTER_STOPPED,
                    handlers.rds_event_handler.RDS_CLUSTER_RESTORED,
                    handlers.rds_event_handler.RDS_CLUSTER_CREATED]

            },
            handlers.ec2_state_event_handler.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: [
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING,
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED]
            }
        },

        ACTION_EVENT_SCOPES: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: {
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED: handlers.EVENT_SCOPE_REGION,
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING: handlers.EVENT_SCOPE_REGION
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
            "rds:AddTagsToResource",
            "rds:RemoveTagsFromResource",
            "rds:DescribeDBInstances",
            "rds:DescribeDBSnapshots",
            "rds:ListTagsForResource",
            "ec2:DescribeTags",
            "tag:GetResources"
        ]

    }

    @property
    def rds_client(self):
        if self._rds_client is None:
            self._rds_client = get_client_with_retries("rds",
                                                       methods=[
                                                           "add_tags_to_resource",
                                                           "remove_tags_from_resource"
                                                       ],
                                                       region=self._region_,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._rds_client

    @staticmethod
    def can_execute(resources, _):
        if not all([any(i in r for i in RESOURCE_ARNS) for r in resources]):
            raise_value_error(ERR_BAD_RESOURCE_TYPE,
                              ",".join([services.rds_service.DB_INSTANCES, services.rds_service.DB_SNAPSHOTS]))

    def __init__(self, action_arguments, action_parameters):
        ActionBase.__init__(self, action_arguments, action_parameters)

        arn_name = None
        for i in RESOURCE_ARNS:
            if i in self._resources_[0]:
                arn_name = i
                break

        self.resource_arns = [res[arn_name] for res in self._resources_ if arn_name in res]

        self._rds_client = None

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "resources": self.resource_arns,
            "source": self._region_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}-{}".format(account, region, log_stream_datetime(), arguments[ACTION_PARAM_TASK_ID])

    def execute(self):
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        tags = self.build_tags_from_template(parameter_name=PARAM_RESOURCE_TAGS,
                                             restricted_value_set=True)

        for arn in self.resource_arns:

            if self.time_out():
                break

            tagging.set_rds_tags(rds_client=self.rds_client,
                                 resource_arns=arn,
                                 tags=tags,
                                 logger=self._logger_)

            self._logger_.info("Set tags {} to resources {}", tags, ','.join(self.resource_arns))

        self.result[METRICS_DATA] = build_action_metrics(self, Resources=len(self.resource_arns), Tags=len(tags))

        return self.result
