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
import copy
import os

import handlers
import handlers.rds_tag_event_handler
import services
import services.rds_service
import tagging
from actions import ACTION_PARAM_EVENTS, ACTION_PARAM_TAG_FILTER
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from tagging.tag_filter_expression import TagFilterExpression

ERR_SET_TAGS = "Can not set tags to RDS instance {}, {}"

WARN_LOOP_TAG = \
    __file__ + "Setting tags {} will trigger task to execute from TaskList tag \"{}={}\", action for instance {} executed " \
               "but tags not set"
WARN_LOOP_TAG_TAGFILTER = \
    "Setting tags {} will trigger task from matching TagFilter \"{}\", actions for instance {} executed but tags not set"
WARN_TAG_FILER_TAG_COMBINATION = \
    "Tag updates in parameter \"{}\":\"{}\" combined with tag filter \"{}\" and RDS tag change event {} could potentially " \
    "trigger execution loop of this task. The new tag values set by this task will be checked before changing the actual tags " \
    "on the resource If the values will trigger a new execution of this task, the tags will not be set."


class ActionRdsEventBase(ActionBase):

    @staticmethod
    def check_tag_filters_and_tags(parameters, task_settings, tag_param_names, logger):

        # check if tag events triggering is used
        task_events = task_settings.get(ACTION_PARAM_EVENTS, {})
        task_change_events = task_events.get(handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE, {}).get(
            handlers.TAG_CHANGE_EVENT, [])
        if handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT in task_change_events not in task_change_events:
            return

        # test for task filter
        tag_filter_str = task_settings.get(handlers.TASK_TR_TAGFILTER, None)
        if tag_filter_str in ["", None]:
            return

            # using any tag parameters
        for p in tag_param_names:
            tags = parameters.get(p, None)
            if tags in [None, ""]:
                continue

            logger.debug(WARN_TAG_FILER_TAG_COMBINATION, p, parameters[p], tag_filter_str,
                         handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT)

        return

    def __init__(self, action_arguments, action_parameters):
        ActionBase.__init__(self, action_arguments, action_parameters)

    def get_db_instance(self, db_instance_id, region=None):
        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        return rds.get(services.rds_service.DB_INSTANCES,
                       DBInstanceIdentifier=db_instance_id,
                       region=region if region is not None else self._region_,
                       tags=True,
                       select="DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier, "
                              "DBInstanceArn:DBInstanceArn ,"
                              "DBInstanceStatus:DBInstanceStatus ,"
                              "Tags:Tags,InstanceId:InstanceId}")

    def set_rds_instance_tags_with_event_loop_check(self, db_instance_id, tags_to_set, client=None, region=None):

        def get_rds_client():
            if client is not None:
                return client

            methods = ["add_tags_to_resource",
                       "remove_tags_from_resource"]

            return get_client_with_retries("rds",
                                           methods=methods,
                                           region=region,
                                           session=self._session_,
                                           logger=self._logger_)

        try:
            if len(tags_to_set) > 0:

                db_instance = self.get_db_instance(db_instance_id)
                if db_instance is None:
                    return

                # before setting the tags check if these tags won't trigger a new execution of the task causing a loop
                task_events = self.get(ACTION_PARAM_EVENTS, {})
                task_change_events = task_events.get(handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE, {}).get(
                    handlers.TAG_CHANGE_EVENT, [])

                if handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT in task_change_events:

                    tag_name = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME)
                    tag_filter_str = self.get(ACTION_PARAM_TAG_FILTER, None)
                    tag_filter = TagFilterExpression(tag_filter_str) if tag_filter_str not in ["", None, "None"] else None

                    # tags currently on instance
                    db_instance_tags = db_instance.get("Tags", {})
                    # tags that have updated values when setting the tags

                    deleted_tags = {t: tags_to_set[t] for t in tags_to_set if
                                    tags_to_set[t] == tagging.TAG_DELETE and t in db_instance_tags}
                    new_tags = {t: tags_to_set[t] for t in tags_to_set if
                                t not in db_instance_tags and tags_to_set[t] != tagging.TAG_DELETE}
                    updated_tags = {t: tags_to_set[t] for t in tags_to_set if
                                    tags_to_set[t] != tagging.TAG_DELETE and t in db_instance_tags and db_instance_tags[t] !=
                                    tags_to_set[t]}

                    updated_tags.update(new_tags)

                    # if there are updates
                    if any([len(t) > 0 for t in [new_tags, updated_tags, deleted_tags]]):

                        # this will be the new set of tags for the instance
                        updated_instance_tags = copy.deepcopy(db_instance_tags)
                        for t in deleted_tags:
                            del updated_instance_tags[t]
                        for t in updated_tags:
                            updated_instance_tags[t] = updated_tags[t]

                        # test if we have a tag filter and if the filter matches the new tags
                        if tag_filter is not None:

                            updated_tags_used_in_filter = set(updated_tags).intersection(tag_filter.get_filter_keys())
                            # tags updated that are in the tag filter
                            if len(updated_tags_used_in_filter) > 0:
                                # test if updated tags trigger the task
                                if tag_filter.is_match(updated_instance_tags):
                                    self._logger_.warning(WARN_LOOP_TAG_TAGFILTER,
                                                          tags_to_set,
                                                          tag_filter_str,
                                                          db_instance["DBInstanceIdentifier"])
                                    return

                        # if no tag filter then check if the tag with the Ops Automator tasks does contain the name of the task
                        else:
                            task_list = updated_instance_tags.get(tag_name, "")
                            if tag_name in updated_tags and self._task_ in tagging.split_task_list(task_list):
                                self._logger_.warning(WARN_LOOP_TAG, tags_to_set, task_list, tag_name,
                                                      db_instance["DBInstanceIdentifier"])
                                return

                tagging.set_rds_tags(rds_client=get_rds_client(),
                                     resource_arns=[db_instance["DBInstanceArn"]],
                                     tags=tags_to_set)

        except Exception as ex:
            self._logger_.error(ERR_SET_TAGS, db_instance_id, str(ex))
