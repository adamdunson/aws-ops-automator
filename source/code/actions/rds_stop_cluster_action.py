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

ERR_STOP_CLUSTER = "Error stopping CLUSTER {}, {}"
ERR_SETTING_CLUSTER_TAGS = "Error setting tags to cluster {}, {}"
ERR_CLUSTER_NOT_FOUND = "Stopped cluster {} does not longer exist"

INF_CLUSTER_STOP_ACTION = "Stopping RDS cluster {} for task {}"
INF_SETTING_CLUSTER_TAGS = "Setting tags {} to cluster {}"
INF_WAIT_MEMBERS_ARE_STOPPED = "Waiting for all cluster members are stopped"

SNAPSHOT_STATUS_CREATING = "creating"
SNAPSHOT_STATUS_AVAILABLE = "available"
SNAPSHOT_STATUS_FAILED = "failed"

CLUSTER_STATUS_STOPPED = "stopped"

GROUP_TITLE_CLUSTER_OPTIONS = "Cluster options"

PARAM_STOPPED_CLUSTER_TAGS = "StoppedClusterTags"

PARAM_DESC_STOPPED_CLUSTER_TAGS = "Tags to set on stopped RDS cluster."

PARAM_LABEL_STOPPED_CLUSTER_TAGS = "Cluster tags"


class RdsStopClusterAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Stop Cluster",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Stops RDS cluster",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "93a78df3-c285-4d8e-98d5-4ce11112a321",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_CLUSTERS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "DBClusters[].{DBClusterIdentifier:DBClusterIdentifier," +
            "Status:Status,"
            "Engine:Engine,"
            "DBClusterArn:DBClusterArn}"
            "|[?contains(['available'],Status)]",

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_CLUSTER_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_STOPPED_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_STOPPED_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_STOPPED_CLUSTER_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_CLUSTER_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_STOPPED_CLUSTER_TAGS
                ],
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:AddTagsToResource",
            "rds:DescribeDBClusters",
            "rds:DescribeDBInstances",
            "rds:ListTagsForResource",
            "rds:RemoveTagsFromResource",
            "rds:StopDBCluster",
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
                                                           "add_tags_to_resource",
                                                           "describe_db_clusters",
                                                           "remove_tags_from_resource",
                                                           "stop_db_cluster"
                                                       ],
                                                       region=self._region_,
                                                       session=self._session_,
                                                       context=self._context_,
                                                       logger=self._logger_)

        return self._rds_client

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        clust_id = resource["DBClusterIdentifier"]

        if not resource["Engine"] in ["aurora"]:
            logger.debug("Unsupported engine for stopping, cluster {} skipped", clust_id)
            return None

        return resource

    def is_completed(self, exec_result):

        def set_tags_to_stopped_cluster(clust_inst):

            tags = self.build_tags_from_template(parameter_name=PARAM_STOPPED_CLUSTER_TAGS, restricted_value_set=True)

            try:
                tagging.set_rds_tags(rds_client=self.rds_client,
                                     resource_arns=[clust_inst["DBClusterArn"]],
                                     tags=tags,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_CLUSTER_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   clust_inst["DBClusterArn"])
            except Exception as tag_ex:
                raise_exception(ERR_SETTING_CLUSTER_TAGS, clust_inst["DBClusterArn"], tag_ex)

        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        db_cluster = rds.get(services.rds_service.DB_CLUSTERS,
                             region=self._region_,
                             DBClusterIdentifier=self.db_cluster_id,
                             _expected_boto3_exceptions_=["DBClusterNotFoundFault"])
        if db_cluster is None:
            raise_exception(ERR_CLUSTER_NOT_FOUND, self.db_cluster_id)

        if db_cluster["Status"] != CLUSTER_STATUS_STOPPED:
            return None

        members = rds.describe(services.rds_service.DB_INSTANCES,
                               region=self._region_,
                               Filters=[
                                   {
                                       "Name": "db-cluster-id", "Values": [self.db_cluster_id]
                                   }
                               ])

        if any([m["DBInstanceStatus"] != "stopped" for m in members]):
            self._logger_.info(INF_WAIT_MEMBERS_ARE_STOPPED)
            return None

        set_tags_to_stopped_cluster(db_cluster)

        return self.result

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_CLUSTER_STOP_ACTION, self.db_cluster_id, self._task_)

        args = {
            "DBClusterIdentifier": self.db_cluster_id
        }

        try:
            self.rds_client.stop_db_cluster_with_retries(**args)
        except Exception as ex:
            raise_exception(ERR_STOP_CLUSTER, self.db_cluster_id, ex)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            StoppedDBClusters=1
        )

        return self.result
