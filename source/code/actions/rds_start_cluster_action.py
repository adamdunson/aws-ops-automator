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


import handlers.rds_tag_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from outputs import raise_exception

ERR_START_CLUSTER = "Error starting cluster {}, {}"
ERR_REMOVING_CLUSTER_TAGS = "Error removing tags from cluster {}, {}"
ERR_SETTING_CLUSTER_TAGS = "Error setting tags to cluster {}, {}"
ERR_CLUSTER_NOT_FOUND = "Started cluster {} does not longer exist"

INF_CLUSTER_START_ACTION = "Starting RDS cluster {} for task {}"
INF_SETTING_CLUSTER_TAGS = "Setting tags {} to cluster {}"
INF_WAIT_MEMBERS_AVAILABLE = "Waiting for all cluster members to become available"

CLUSTER_STATUS_AVAILABLE = "available"

GROUP_TITLE_CLUSTER_OPTIONS = "Cluster options"

PARAM_STARTED_CLUSTER_TAGS = "StartedClusterTags"

PARAM_DESC_STARTED_CLUSTER_TAGS = \
    "Tags to set on started RDS cluster. Note that tag values for RDS cannot contain ',' characters. When specifying multiple " \
    "follow up tasks in the value of the Ops Automator task list tag use a '/' character instead"

PARAM_LABEL_STARTED_CLUSTER_TAGS = "Cluster tags"


class RdsStartClusterAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Start Cluster",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Starts RDS Aurora cluster",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "042daed6-f1b0-404d-bd16-24dbfc46dc37",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_CLUSTERS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_ALLOW_TAGFILTER_WILDCARD: True,

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_CLUSTER_TAGS_EVENT]
            }
        },

        ACTION_SELECT_EXPRESSION:
            "DBClusters[].{DBClusterIdentifier:DBClusterIdentifier," +
            "Status:Status,"
            "MultiAZ:MultiAZ," +
            "Engine:Engine,"
            "DBClusterArn:DBClusterArn}"
            "|[?contains(['stopped'],Status)]",

        ACTION_PARAMETERS: {

            PARAM_STARTED_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STARTED_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STARTED_CLUSTER_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_CLUSTER_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STARTED_CLUSTER_TAGS
                ],
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:AddTagsToResource",
            "rds:DescribeDBClusters",
            "rds:DescribeDBInstances",
            "rds:RemoveTagsFromResource",
            "rds:ListTagsForResource",
            "rds:StartDBCluster",
            "tag:GetResources"
        ]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_cluster = self._resources_

        self.db_cluster_id = self.db_cluster["DBClusterIdentifier"]
        self.db_cluster_arn = self.db_cluster["DBClusterArn"]
        self._rds_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "db-cluster": self.db_cluster_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        db_cluster = arguments[ACTION_PARAM_RESOURCES]
        db_cluster_id = db_cluster["DBClusterIdentifier"]
        account = db_cluster["AwsAccount"]
        region = db_cluster["Region"]
        return "{}-{}-{}-{}".format(account, region, db_cluster_id, log_stream_date())

    @property
    def rds_client(self):

        if self._rds_client is None:
            self._rds_client = get_client_with_retries("rds",
                                                       methods=[
                                                           "describe_db_clusters",
                                                           "add_tags_to_resource",
                                                           "remove_tags_from_resource",
                                                           "start_db_cluster"],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       context=self._context_)

        return self._rds_client

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        cl_id = resource["DBClusterIdentifier"]

        if not resource["Engine"] in ["aurora"]:
            logger.debug("Unsupported engine type for started cluster, cluster {} skipped", cl_id)
            return None
        return resource

    def is_completed(self, _):

        def set_tags_to_started_cluster(db_inst):

            # tags on the started cluster
            tags = self.build_tags_from_template(parameter_name=PARAM_STARTED_CLUSTER_TAGS, restricted_value_set=True)

            try:
                tagging.set_rds_tags(rds_client=self.rds_client,
                                     resource_arns=[db_inst["DBClusterArn"]],
                                     tags=tags,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_CLUSTER_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   db_inst["DBClusterArn"])
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_CLUSTER_TAGS, db_inst["DBClusterArn"], tag_ex)

        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        db_cluster = rds.get(services.rds_service.DB_CLUSTERS,
                             region=self._region_,
                             DBClusterIdentifier=self.db_cluster_id,
                             tags=False,
                             _expected_boto3_exceptions_=["DBClusterNotFoundFault"])

        if db_cluster is None:
            raise_exception(ERR_CLUSTER_NOT_FOUND, self.db_cluster_id)

        if db_cluster["Status"] != CLUSTER_STATUS_AVAILABLE:
            return None

        members = rds.describe(services.rds_service.DB_INSTANCES,
                               region=self._region_,
                               Filters=[{"Name": "db-cluster-id", "Values": [self.db_cluster_id]}])

        if not all([m["DBInstanceStatus"] == "available" for m in members]):
            self._logger_.info(INF_WAIT_MEMBERS_AVAILABLE)
            return None

        set_tags_to_started_cluster(db_cluster)

        return self.result

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_CLUSTER_START_ACTION, self.db_cluster_id, self._task_)

        try:
            self.rds_client.start_db_cluster_with_retries(DBClusterIdentifier=self.db_cluster_id)
        except Exception as ex:
            raise_exception(ERR_START_CLUSTER, self.db_cluster_id, ex)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StartedDBClusters=1
        )

        return self.result
