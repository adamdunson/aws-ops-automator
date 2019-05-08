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

import StringIO
import csv

import handlers.ebs_snapshot_event_handler
import handlers.ec2_state_event_handler
import handlers.ec2_tag_event_handler
import services.ec2_service
import services.tagging_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_value_error
from outputs.report_output_writer import report_key_name
from tagging.tag_filter_expression import TagFilterExpression

INF_NON_COMPLIANT_RESOURCE = "Resource {} is not compliant as its tags do not match tag filter \"{}\""
INF_SHARED_NOT_CHECKED = "Shared snapshot {}, owner by account {}, is not checked for task {}"
INF_STOPPING_INSTANCES = "Stopping non-compliant EC2 instances {}"
INF_UNCHECKED_TYPE = "Resource {}, type {} is not checked for task {}"
INF_COMPLIANT = "Resource is tag compliant"
INF_CHECKED_RESOURCE = "Checking tag compliancy for resource {} {}"

ERR_PARAM_EBS_SNAPSHOTS_EVENT = "{} must be enabled if any of the EBS snapshot events {} is used"
ERR_PARAM_EC2_STATE_EVENT = "{} must be enabled if any of the EC2 state events {} is used"
ERR_PARAM_MUST_BE_ENABLED_IF = "{} must be enabled if {} is enabled"
ERR_PARAM_NO_ACTIONS = "No actions are defined, at least {} or {} must be enabled or {} must be set {}"
ERR_PARAM_NO_CHECKS = "At least one check from {} must be enabled"
ERR_PARAM_STOP_INSTANCES = "{} action can only be used if {} is enabled"
ERR_PARAM_TAGGING_EVENTS = "Check{} must be enabled when using any of the tagging add or remove events for type {}"
ERR_STOPPING_INSTANCES = "Error stopping non-compliant EC2 instances, {}"
ERR_TAGGING_RESOURCES = "Error setting tags {} to non-compliant EC2 resources"

WARN_UNKNOWN_RESOURCE_TYPE = "Resource type of resource {} is not supported"

PARAM_COMPLIANCY_TAG_FILTER = "CompliancyTagFilter"
PARAM_CHECK_IMAGES = "CheckImages"
PARAM_CHECK_INSTANCES = "CheckInstances"
PARAM_CHECK_SNAPSHOTS = "CheckSnapshots"
PARAM_CHECK_VOLUMES = "CheckVolumes"
PARAM_RESOURCE_TAGS = "SetTags"
PARAM_STOP_INSTANCE = "StopInstance"
PARAM_REPORT = "WriteOutputReport"

PARAM_DESC_COMPLIANCY_TAG_FILTER = "Tag filter expression that tags of selected resources must match to be compliant."
PARAM_DESC_CHECK_IMAGES = "Check tags of owned Amazon Machine Images."
PARAM_DESC_CHECK_INSTANCES = "Check tags of running EC2 Instances."
PARAM_DESC_CHECK_SNAPSHOTS = "Check tags of available EBS Snapshots."
PARAM_DESC_CHECK_VOLUMES = "Check tags of EBS Volumes"
PARAM_DESC_RESOURCE_TAGS = "Tags to add to non-compliant resources as a comma delimited list of name=value pairs."
PARAM_DESC_STOP_INSTANCE = "Stop running non-compliant EC2 instances."
PARAM_DESC_REPORT = "Create CSV output report including non-compliant instance data."

PARAM_LABEL_COMPLIANCY_TAG_FILTER = "Compliancy check tag filter"
PARAM_LABEL_CHECK_IMAGES = "Images"
PARAM_LABEL_CHECK_INSTANCES = "EC2 Instances"
PARAM_LABEL_CHECK_SNAPSHOTS = "EBS Snapshots"
PARAM_LABEL_CHECK_VOLUMES = "EBS Volumes"
PARAM_LABEL_RESOURCE_TAGS = "Set Tags"
PARAM_LABEL_STOP_INSTANCE = "Stop Instance"
PARAM_LABEL_REPORT = "Create CSV report"

ERR_BAD_RESOURCE_TYPE = "Action can only check tags for EC2 resources types {}"

GROUP_CHECKED_RESOURCES_TITLE = "Checked Resource types"
GROUP_LABEL_TAG_FILTERS = "Tag Filter"
GROUP_LABEL_ACTIONS = "Actions"

HANDLED_RESOURCE_TYPES = [services.ec2_service.INSTANCES,
                          services.ec2_service.SNAPSHOTS,
                          services.ec2_service.VOLUMES,
                          services.ec2_service.IMAGES]


class Ec2CheckTagsAction(ActionBase):
    """
    Class implements checking tags of of selected EC2 resources
    """

    properties = {
        ACTION_TITLE: "EC2 Check Tags",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPTION: "Checks tags for EC2 Instances, EBS Snapshots, Volumes and Images",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "9e6a5867-c982-4886-85c1-a0176a0d2223",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,
        ACTION_BATCH_SIZE: 50,

        ACTION_MIN_INTERVAL_MIN: 5,

        ACTION_NO_TAG_SELECT: False,

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION: [
                    handlers.ec2_state_event_handler.EC2_STATE_RUNNING,
                    handlers.ec2_state_event_handler.EC2_STATE_STOPPED]
            }
        },

        ACTION_TRIGGERS: ACTION_TRIGGER_BOTH,

        ACTION_PARAMETERS: {

            PARAM_RESOURCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_RESOURCE_TAGS,
                PARAM_LABEL: PARAM_LABEL_RESOURCE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },

            PARAM_STOP_INSTANCE: {
                PARAM_DESCRIPTION: PARAM_DESC_STOP_INSTANCE,
                PARAM_LABEL: PARAM_LABEL_STOP_INSTANCE,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False
            },

            PARAM_COMPLIANCY_TAG_FILTER: {
                PARAM_DESCRIPTION: PARAM_DESC_COMPLIANCY_TAG_FILTER,
                PARAM_LABEL: PARAM_LABEL_COMPLIANCY_TAG_FILTER,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_CHECK_INSTANCES: {
                PARAM_DESCRIPTION: PARAM_DESC_CHECK_INSTANCES,
                PARAM_LABEL: PARAM_LABEL_CHECK_INSTANCES,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False
            },
            PARAM_CHECK_VOLUMES: {
                PARAM_DESCRIPTION: PARAM_DESC_CHECK_VOLUMES,
                PARAM_LABEL: PARAM_LABEL_CHECK_VOLUMES,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False
            },
            PARAM_CHECK_SNAPSHOTS: {
                PARAM_DESCRIPTION: PARAM_DESC_CHECK_SNAPSHOTS,
                PARAM_LABEL: PARAM_LABEL_CHECK_SNAPSHOTS,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False
            },
            PARAM_CHECK_IMAGES: {
                PARAM_DESCRIPTION: PARAM_DESC_CHECK_IMAGES,
                PARAM_LABEL: PARAM_LABEL_CHECK_IMAGES,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False
            },
            PARAM_REPORT: {
                PARAM_DESCRIPTION: PARAM_DESC_REPORT,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_REPORT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_CHECKED_RESOURCES_TITLE,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CHECK_INSTANCES,
                    PARAM_CHECK_VOLUMES,
                    PARAM_CHECK_SNAPSHOTS,
                    PARAM_CHECK_IMAGES
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_TAG_FILTERS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COMPLIANCY_TAG_FILTER,
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_ACTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STOP_INSTANCE,
                    PARAM_RESOURCE_TAGS,
                    PARAM_REPORT
                ],
            }
        ],

        ACTION_PERMISSIONS: ["ec2:DescribeInstances",
                             "ec2:DescribeSnapshots",
                             "ec2:DescribeVolumes",
                             "ec2:DescribeImages",
                             "ec2:StopInstances",
                             "ec2:CreateTags",
                             "ec2:DeleteTags"]

    }

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task, logger):

        # checked resource types
        checks = {c: parameters.get("Check{}".format(c), False) for c in HANDLED_RESOURCE_TYPES}

        # need at least one checked resource
        if not any(list(checks.values())):
            raise_value_error(ERR_PARAM_NO_CHECKS, ",".join(["Check" + t for t in HANDLED_RESOURCE_TYPES]))

        events = task.get("Events")

        # if any EC2 state events are used then instance checking must be enabled
        ec2_state_events = events.get(handlers.EC2_EVENT_SOURCE, {}). \
            get(handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION, [])
        if len(ec2_state_events) > 0 and not checks["Instances"]:
            raise_value_error(ERR_PARAM_EC2_STATE_EVENT, PARAM_CHECK_INSTANCES,
                              ", ".join(handlers.ec2_state_event_handler.HANDLED_EVENTS["events"]))

        # stop instance action is only allows if instance checking is enabled
        if parameters.get(PARAM_STOP_INSTANCE, False) and not checks["Instances"]:
            raise_value_error(ERR_PARAM_STOP_INSTANCES, PARAM_STOP_INSTANCE, PARAM_CHECK_INSTANCES)

        return parameters

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        resource_type = resource["ResourceTypeName"]
        if resource_type not in HANDLED_RESOURCE_TYPES:
            logger.warning(WARN_UNKNOWN_RESOURCE_TYPE, safe_json(resource, indent=3))
            return None

        resource_id = resource[resource_type[0:-1] + "Id"]

        if not task.get(handlers.TASK_PARAMETERS, {}).get("Check{}".format(resource_type)):
            logger.info(INF_UNCHECKED_TYPE, resource_id, resource_type, task[handlers.TASK_NAME])
            return None

        result = {a: resource[a] for a in ["AwsAccount", "Region", "Tags", "ResourceTypeName"]}
        result["Id"] = resource_id

        return result

    @staticmethod
    def can_execute(resources, _):
        if not all([r["ResourceTypeName"] in HANDLED_RESOURCE_TYPES for r in resources]):
            raise_value_error(ERR_BAD_RESOURCE_TYPE, ",".join(HANDLED_RESOURCE_TYPES))

    @staticmethod
    def describe_resources(service, task, region):
        resources = []

        if task[handlers.TASK_PARAMETERS][PARAM_CHECK_INSTANCES]:
            resources += list(service.describe(services.ec2_service.INSTANCES,
                                               region=region,
                                               select="Reservations[*].Instances[]."
                                                      "{State:State.Name,InstanceId:InstanceId, Tags:Tags} "
                                                      "|[?State !='terminated']"))

        if task[handlers.TASK_PARAMETERS][PARAM_CHECK_VOLUMES]:
            resources += list(service.describe(services.ec2_service.VOLUMES,
                                               region=region,
                                               select="Volumes[].{VolumeId:VolumeId, Tags:Tags}"))

        if task[handlers.TASK_PARAMETERS][PARAM_CHECK_SNAPSHOTS]:
            resources += list(service.describe(services.ec2_service.SNAPSHOTS,
                                               region=region,
                                               select="Snapshots[?State=='completed']."
                                                      "{SnapshotId:SnapshotId, Tags:Tags}",
                                               OwnerIds=["self"]))

        if task[handlers.TASK_PARAMETERS][PARAM_CHECK_IMAGES]:
            resources += list(service.describe(services.ec2_service.IMAGES,
                                               region=region,
                                               select="Images[?State=='available']."
                                                      "{ImageId:ImageId, Tags:Tags}", Owners=["self"]))
        return resources

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=["stop_instances",
                                                                "create_tags",
                                                                "delete_tags"],
                                                       region=self._region_,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @property
    def report_writer(self):
        if self._report_writer is None:
            self._report_writer = get_report_output_writer(context=self._context_, logger=self._logger_)
        return self._report_writer

    def _stop_instances(self):

        if len(self.stopped_instances) == 0:
            return
        try:
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2",
                                                                                            context=self._context_))

            running_instances_to_stop = [i["InstanceId"] for i in ec2.describe(
                services.ec2_service.INSTANCES,
                InstanceIds=self.stopped_instances,
                region=self._region_,
                select="Reservations[*].Instances[].{State:State.Name,InstanceId:InstanceId}") if
                                         i["State"] in ["running", "pending"]]

            if len(running_instances_to_stop) > 0:
                self._logger_.info(INF_STOPPING_INSTANCES, ",'".join(running_instances_to_stop))
                self.ec2_client.stop_instances_with_retries(InstanceIds=running_instances_to_stop)

        except Exception as ex:
            self._logger_.error(ERR_STOPPING_INSTANCES, ex)

    def _tag_non_compliant_resources(self):

        if len(self.tagged_resources) == 0:
            return

        tags_to_set = self.build_tags_from_template(parameter_name=PARAM_RESOURCE_TAGS)

        try:
            if len(tags_to_set) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=self.tagged_resources,
                                     tags=tags_to_set,
                                     logger=self._logger_)

        except Exception as ex:
            self._logger_.error(ERR_TAGGING_RESOURCES, tags_to_set, ','.join(self.tagged_resources), str(ex))

    def _create_report(self):

        s = StringIO.StringIO()
        csv_data = csv.writer(s)

        # build header row
        instance_data_headers = ["AwsAccount", "Region", "ResourceId", "ResourceType", "RequiredTags", "Tags"]

        csv_data.writerow(["DateTime"] + instance_data_headers)

        # date and time
        dt = self._datetime_.now().replace(second=0, microsecond=0)
        date_time = [dt.isoformat()]

        for resource in self.non_compliant_resources:
            resource_id = resource["Id"]
            resource_type = resource["ResourceTypeName"][0:-1]
            tags = resource.get("Tags", [])
            tag_data = ",".join(["{}={}".format(t, tags[t]) for t in tags])
            resource_row_data = [resource["AwsAccount"],
                                 resource["Region"], resource_id,
                                 resource_type,
                                 self.tag_filter.filter_expression, tag_data]

            # add the row
            row = date_time + resource_row_data
            csv_data.writerow(row)

        self.report_writer.write(s.getvalue(), report_key_name(self))

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self._ec2_client = None

        compliancy_tag_filter = self.get(PARAM_COMPLIANCY_TAG_FILTER, "").strip()
        self.tag_filter = TagFilterExpression(compliancy_tag_filter) if compliancy_tag_filter not in ["", None] else None

        self.stop_non_compliant_instances = self.get(PARAM_STOP_INSTANCE, False)
        self.report = self.get(PARAM_REPORT, False)

        self._report_writer = None
        self.stopped_instances = []
        self.tagged_resources = []
        self.non_compliant_resources = []
        self.non_compliant_count = 0

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "region": self._region_
        }

    def execute(self):

        def handle_non_compliant_resource(resource):

            if self.stop_non_compliant_instances and resource["ResourceTypeName"] == services.ec2_service.INSTANCES:
                self.stopped_instances.append(resource["Id"])

            self.tagged_resources.append(resource["Id"])

            self.non_compliant_resources.append(resource)

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        for r in self._resources_:

            if self.time_out():
                break

            self._logger_.info(INF_CHECKED_RESOURCE, r["ResourceTypeName"][0:-1], r["Id"])
            tags = r.get("Tags", {})

            if self.tag_filter is not None:
                if len(tags) == 0 or not self.tag_filter.is_match(tags):
                    self._logger_.info(INF_NON_COMPLIANT_RESOURCE, str(r), self.tag_filter.filter_expression)
                    handle_non_compliant_resource(r)
                else:
                    self._logger_.info(INF_COMPLIANT)

        self._stop_instances()
        self._tag_non_compliant_resources()

        if self.report:
            self._create_report()

        self.result["checked-resources"] = len(self._resources_)
        self.result["non-compliant-resources"] = [r["Id"] for r in self.non_compliant_resources]
        self.result["stopped-instances"] = self.stopped_instances
        self.result["tagged-resources"] = len(self.tagged_resources)

        self.result[METRICS_DATA] = build_action_metrics(self, CheckedResources=len(self._resources_))

        return self.result
