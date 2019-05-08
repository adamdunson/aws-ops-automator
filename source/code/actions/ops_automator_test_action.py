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
import random
import time
from decimal import *

from action_base import ActionBase
from actions import *
from helpers import safe_json
from scheduling.setbuilder import SetBuilder

RESOURCE_ID_FORMAT = "res-{:0>16d}"

PARAM_TEST_AGGREGATION = "Aggregation"
PARAM_TEST_BATCH_SIZE = "BatchSize"
PARAM_TEST_COMPLETE_CHECK_FAILING = "CompleteCheckFailing"
PARAM_TEST_COMPLETION_CHECK_DURATION = "CompletionCheckDuration"
PARAM_TEST_COMPLETION_CHECK_DURATION_VARIANCE = "CheckCompletionVariance"
PARAM_TEST_EXECUTE_DURATION = "ExecuteDuration"
PARAM_TEST_EXECUTE_DURATION_VARIANCE = "ExecuteVariance"
PARAM_TEST_FAILING_EXECUTE_RESOURCES = "FailExecuteResources"
PARAM_TEST_HAS_COMPLETION = "HasCompletion"
PARAM_TEST_NAME = "TestName"
PARAM_TEST_RESOURCES = "TestResources"
PARAM_TEST_SELECT_DURATION = "SelectDuration"
PARAM_TEST_SELECT_DURATION_VARIANCE = "SelectVariance"
PARAM_TEST_SELECT_FAILING = "SelectFailing"
PARAM_TEST_SELECT_TAGS = "TestResourcesTags"
PARAM_TEST_MAX_CONCURRENT = "MaxConcurrent"

TEST_MAX_RESOURCES = 1000

TEST_RESOURCE_ID = "ResourceId"
TEST_RESOURCE_NAMES = ["TestResources"]


class OpsAutomatorTestAction(ActionBase):
    properties = {
        ACTION_TITLE: "Test Action",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Internal action to test Ops Automator Core Framework",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "e5b42606-9af9-49c3-8688-a74435e3b5a9",

        ACTION_INTERNAL: True,

        ACTION_SERVICE: "opsautomatortest",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: lambda parameters: parameters.get(PARAM_TEST_AGGREGATION, ACTION_AGGREGATION_RESOURCE),

        ACTION_BATCH_SIZE: lambda parameters: parameters.get(PARAM_TEST_BATCH_SIZE, None),

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,
        ACTION_EXECUTE_SIZE: ACTION_SIZE_ALL_WITH_ECS,
        ACTION_COMPLETION_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_MAX_CONCURRENCY: lambda parameters: int(parameters.get(PARAM_TEST_MAX_CONCURRENT, 0)),

        ACTION_PARAMETERS: {
            PARAM_TEST_NAME: {
                PARAM_DESCRIPTION: "Name of the test",
                PARAM_TYPE: str,
                PARAM_REQUIRED: True
            },
            PARAM_TEST_RESOURCES: {
                PARAM_DESCRIPTION: "Test resources spec",
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,
                PARAM_DESCRIBE_PARAMETER: PARAM_TEST_RESOURCES
            },
            PARAM_TEST_AGGREGATION: {
                PARAM_DESCRIPTION: "Test resources aggregation",
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: PARAM_TEST_AGGREGATION
            },
            PARAM_TEST_BATCH_SIZE: {
                PARAM_DESCRIPTION: "Test resources batch size",
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
            },
            PARAM_TEST_SELECT_TAGS: {
                PARAM_DESCRIPTION: "Test resources tags",
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_DESCRIBE_PARAMETER: PARAM_TEST_SELECT_TAGS
            },
            PARAM_TEST_SELECT_DURATION: {
                PARAM_DESCRIPTION: "Duration of selecting test resources in seconds",
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0,
                PARAM_DESCRIBE_PARAMETER: PARAM_TEST_SELECT_DURATION
            },
            PARAM_TEST_SELECT_DURATION_VARIANCE: {
                PARAM_DESCRIPTION: "Variance in duration of selecting test resources (0-1)",
                PARAM_TYPE: Decimal,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0.0,
                PARAM_DESCRIBE_PARAMETER: PARAM_TEST_SELECT_DURATION_VARIANCE
            },
            PARAM_TEST_SELECT_FAILING: {
                PARAM_DESCRIPTION: "Make select step fail",
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_DESCRIBE_PARAMETER: PARAM_TEST_SELECT_FAILING
            },
            PARAM_TEST_EXECUTE_DURATION: {
                PARAM_DESCRIPTION: "Duration of execution for on test resources in seconds",
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0,
            },
            PARAM_TEST_FAILING_EXECUTE_RESOURCES: {
                PARAM_DESCRIPTION: "Resources for which execution fails",
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_TEST_EXECUTE_DURATION_VARIANCE: {
                PARAM_DESCRIPTION: "Variance in duration of executing on test resources (0-1)",
                PARAM_TYPE: Decimal,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0.0,
            },
            PARAM_TEST_COMPLETION_CHECK_DURATION: {
                PARAM_DESCRIPTION: "Duration of completion check for on test resources in seconds",
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0,
            },
            PARAM_TEST_COMPLETE_CHECK_FAILING: {
                PARAM_DESCRIPTION: "Resources for which check completion fails fails",
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_TEST_COMPLETION_CHECK_DURATION_VARIANCE: {
                PARAM_DESCRIPTION: "Variance in duration of executing on test resources (0-1)",
                PARAM_TYPE: Decimal,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0.0,
            },
            PARAM_TEST_HAS_COMPLETION: {
                PARAM_DESCRIPTION: "Switch for enabling completion checking",
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
            },
            PARAM_TEST_MAX_CONCURRENT: {
                PARAM_DESCRIPTION: "Max concurrent executions",
                PARAM_TYPE: int,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: 0,
            },
        },

        ACTION_PARAMETER_GROUPS: [
        ],

        ACTION_PERMISSIONS: ["None:None"],

        ACTION_INTERNAL: True

    }

    def __init__(self, action_args, action_parameters):
        ActionBase.__init__(self, action_args, action_parameters)

    @staticmethod
    def action_logging_subject(arguments, parameters):

        fields = []
        test_name = parameters.get(PARAM_TEST_NAME)
        if test_name != "":
            fields.append(test_name)

        aggregation = OpsAutomatorTestAction.properties[ACTION_AGGREGATION](parameters)

        if aggregation in [ACTION_AGGREGATION_ACCOUNT, ACTION_AGGREGATION_REGION]:
            fields.append(arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"])

        if aggregation in [ACTION_AGGREGATION_REGION]:
            fields.append(arguments[ACTION_PARAM_RESOURCES][0]["Region"])

        if aggregation == ACTION_AGGREGATION_RESOURCE:
            fields.append(arguments[ACTION_PARAM_RESOURCES]["AwsAccount"])
            fields.append(arguments[ACTION_PARAM_RESOURCES]["Region"])
            fields.append(arguments[ACTION_PARAM_RESOURCES]["ResourceId"])

        fields.append(log_stream_datetime())
        fields.append(arguments[ACTION_PARAM_TASK_ID])

        return "-".join(fields)

    @staticmethod
    def has_completion(parameters):
        return str(parameters.get(PARAM_TEST_HAS_COMPLETION, True)).lower() == "true"

    def _step_failing_resources(self, step):
        f = self.get(step, None)
        if f is None:
            return set()
        if isinstance(f, int):
            return {RESOURCE_ID_FORMAT.format(f)}
        if isinstance(f, basestring):
            return {RESOURCE_ID_FORMAT.format(i) for i in SetBuilder(min_value=0, max_value=TEST_MAX_RESOURCES).build(f)}
        raise ValueError("{} in step {} has invalid value".format(PARAM_TEST_SELECT_FAILING, step))

    @property
    def failing_execution_resources(self):
        return self._step_failing_resources(PARAM_TEST_FAILING_EXECUTE_RESOURCES)

    @property
    def failing_completion_resources(self):
        return self._step_failing_resources(PARAM_TEST_COMPLETE_CHECK_FAILING)

    def is_completed(self, start_results):

        # start_data = json.loads(start_results)
        start_data = start_results
        resources = start_data.get("processed-resources", [])

        start = self._datetime_.now().replace(microsecond=0)

        for res_id in resources:
            self._logger_.info("Executing completion checking for resource {}", res_id)
            failed = self.failing_completion_resources
            if res_id in failed:
                self._logger_.info("Resources {} is in list of resources for which completion should fail", res_id)
                raise Exception("Completion checking for resource {} failed".format(res_id))

        completion_duration = int(self.get(PARAM_TEST_COMPLETION_CHECK_DURATION, 0))

        if completion_duration != 0:
            variance = float(self.get(PARAM_TEST_COMPLETION_CHECK_DURATION_VARIANCE, 0))
            if variance != 0:
                completion_duration += (random.uniform(variance * -1, variance) * completion_duration)
            time_spend = (datetime.now() - start).total_seconds()
            if time_spend < completion_duration:
                wait_time = completion_duration - time_spend
                self._logger_.info("Suspending {:.1f} seconds to emulate completion duration of {:.1f} seconds", wait_time,
                                   completion_duration)
                time.sleep(wait_time)

        return start_results

    def execute(self):

        self._logger_.info("{}, version {}", str(self.__class__).split(".")[-1], self.properties[ACTION_VERSION])
        self._logger_.debug("Implementation {}", __name__)

        start = self._datetime_.now().replace(microsecond=0)

        for res in self._resources_ if isinstance(self._resources_, list) else [self._resources_]:
            self._logger_.info("Executing test action for resource {}", safe_json(res, indent=3))
            failed = self.failing_execution_resources
            if res[TEST_RESOURCE_ID] in failed:
                self._logger_.info("Resources {} is in list of resources for which execution should fail", res[TEST_RESOURCE_ID])
                raise Exception("Execution for resource {} failed".format(res[TEST_RESOURCE_ID]))

        execution_duration = int(self.get(PARAM_TEST_EXECUTE_DURATION, 0))

        if execution_duration != 0:
            variance = float(self.get(PARAM_TEST_EXECUTE_DURATION_VARIANCE, 0))
            if variance != 0:
                execution_duration += (random.uniform(variance * -1, variance) * execution_duration)
            time_spend = (datetime.now() - start).total_seconds()
            if time_spend < execution_duration:
                wait_time = execution_duration - time_spend
                self._logger_.info("Suspending {:.1f} seconds to emulate execution duration of {:.1f} seconds", wait_time,
                                   execution_duration)
                time.sleep(wait_time)

        # noinspection PyTypeChecker
        return {
            "processed-resources": [r[TEST_RESOURCE_ID] for r in
                                    (self._resources_ if isinstance(self._resources_, list) else [self._resources_])],
            "execution-started": start,
            "execution-finished": self._datetime_.now().replace(microsecond=0)

        }
