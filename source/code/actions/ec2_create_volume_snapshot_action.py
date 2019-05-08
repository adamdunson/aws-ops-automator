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


import actions
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging.tag_filter_set import TagFilterSet

INFO_CHECKING_STATUS = "Checking status of snapshot {}"

TAG_PLACEHOLDER_VOLUME_ID = "volume-id"
TAG_PLACEHOLDER_SNAPSHOT = "snapshot-id"

SNAPSHOT_STATE_ERROR = "error"
SNAPSHOT_STATE_PENDING = "pending"
SNAPSHOT_STATE_COMPLETED = "completed"

GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot volume options"
GROUP_TITLE_TAGGING = "Tagging options"
GROUP_TITLE_NAMING = "Snapshot naming and description"

PARAM_DESC_SNAPSHOT_DESCRIPTION = "Description for snapshot,leave blank for default description."
PARAM_DESC_SHARED_ACCOUNT_TAGGING_ROLENAME = \
    "Name of the cross account role in the accounts the snapshot is shared with, that is used to create tags in these " \
    "accounts for the shared snapshot. Leave this parameter empty to use the default role with name \"{}\". The role must give " \
    "permissions to use the Ec2SetTags action."
PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "List of accounts that will be granted access to create volumes from the snapshot."
PARAM_DESC_COPIED_VOLUME_TAGS = "Enter a tag filter to copy tags from the volume to the snapshot.\
                                 For example, enter * to copy all tags from the volume to the snapshot."
PARAM_DESC_SET_SNAPSHOT_NAME = "Set name of the snapshot"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for snapshot name"
PARAM_DESC_SNAPSHOT_TAGS = "Tags that will be added to created snapshots. Use a list of tagname=tagvalue pairs."
PARAM_DESC_VOLUME_TAGS = "Tags to set on source EBS Volume after the snapshots has been created successfully."
PARAM_DESC_NAME = "Name of the created snapshot, leave blank for default snapshot name"

PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "Accounts with create volume permissions"
PARAM_LABEL_SHARED_ACCOUNT_TAGGING_ROLENAME = "Cross account role name for tagging of shared snapshots"
PARAM_LABEL_COPIED_VOLUME_TAGS = "Copied volume tags"
PARAM_LABEL_SNAPSHOT_DESCRIPTION = "Snapshot description"
PARAM_LABEL_NAME = "Snapshot name"
PARAM_LABEL_SET_SNAPSHOT_NAME = "Set snapshot name"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_VOLUME_TAGS = "Volume tags"

PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "CreateVolumePermission"
PARAM_COPIED_VOLUME_TAGS = "CopiedVolumeTags"
PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME = "TagInSharedAccountRoleName"
PARAM_NAME = "SnapshotName"
PARAM_SET_SNAPSHOT_NAME = "SetSnapshotName"
PARAM_SNAPSHOT_DESCRIPTION = "SnapshotDescription"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_VOLUME_TAGS = "VolumeTags"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

INFO_COMPLETED = "Creation of snapshot completed"
INFO_CREATE_SNAPSHOT = "Creating snapshot for volume {}"
INFO_CREATE_TAGS = "Creating tags {} for snapshot"
INFO_CREATION_PENDING = "Creation of snapshot in progress but not completed yet"
INFO_NOT_CREATED_YET = "Snapshot has not been been created yet"
INFO_SET_VOLUME_TAGS = "Set tags {} to volume {}"
INFO_SETTING_CREATE_VOLUME_PERMISSIONS = "Setting create volume permissions for {}"
INFO_SNAPSHOT_CREATED = "Snapshot is {}"
INFO_SNAPSHOT_NAME = "Name of the snapshot will be set to {}"
INFO_STATE_SNAPSHOT = "State of created snapshot is\n{}"
INFO_TAGS_CREATED = "Snapshots tags created"
INFO_START_SNAPSHOT_ACTION = "Creating snapshot for volume {} for account {} in region {} using task {}"
INFO_PENDING_SNAPSHOTS = "Volume {} has a pending snapshots {}"
INFO_SET_SNAPSHOT_TAGS_SHARED = "Set tags\n{}\nto snapshot {} in account {}"

ERR_FAILED_SNAPSHOT = "Error creating snapshot {} for volume {}"
ERR_SETTING_CREATE_VOLUME_PERMISSIONS = "Error setting create volume permissions for account(s) {}, {}"
ERR_SETTING_VOLUME_TAGS = "Error setting tags to volume {}, {}"
ERR_SETTING_SHARED_TAGS = "Can not set tags for shared EBS volume snapshot in account {}, {}"
ERR_SNAPSHOT_PENDING = "Volume {} already has pending snapshots, no snapshot will be taken for this volume"
ERR_TAGS_NOT_SET_IN_ACCOUNT = "Tags not set in account {}"

WARN_ROOT_NOT_FOUND = "Root device for instance {} not backed up as it could not be found. devices are {}."

SNAPSHOT_DESCRIPTION = "Snapshot created by task {} for volume {}"


class Ec2CreateVolumeSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Create Volume Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Creates snapshot for an EBS Volume",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "0d3e0901-5189-40ea-83ba-55706c4bda0a",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.VOLUMES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_EXPRESSION: "Volumes[*].{VolumeId:VolumeId,Tags:Tags}",

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_PARAMETERS: {

            PARAM_COPIED_VOLUME_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_VOLUME_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_VOLUME_TAGS
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_SET_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SET_SNAPSHOT_NAME,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_SET_SNAPSHOT_NAME
            },
            PARAM_SNAPSHOT_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME_PREFIX
            },
            PARAM_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_NAME
            },
            PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS
            },
            PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SHARED_ACCOUNT_TAGGING_ROLENAME.format(handlers.default_rolename_for_stack()),
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SHARED_ACCOUNT_TAGGING_ROLENAME
            },
            PARAM_VOLUME_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_VOLUME_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_VOLUME_TAGS
            },
            PARAM_SNAPSHOT_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_DESCRIPTION,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_DESCRIPTION
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                    PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SET_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_NAME,
                    PARAM_SNAPSHOT_DESCRIPTION
                ]
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_VOLUME_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_VOLUME_TAGS
                ],
            }
        ],

        ACTION_PERMISSIONS: ["ec2:CreateSnapshot",
                             "ec2:DescribeVolumes",
                             "ec2:DescribeSnapshots",
                             "ec2:ModifySnapshotAttribute",
                             "ec2:CreateTags",
                             "ec2:DeleteTags"],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.volume = self._resources_
        self.volume_id = self.volume["VolumeId"]
        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        self._ec2_client = None

        self.accounts_with_create_permissions = self.get(PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS, [])
        self.cross_account_shared_snapshot_tagging = self.get(PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME, [])

        self.copied_volume_tagfilter = TagFilterSet(self.get(PARAM_COPIED_VOLUME_TAGS, ""))

        self.set_snapshot_name = self.get(PARAM_SET_SNAPSHOT_NAME, True)

        self._all_volume_tags = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "volume": self.volume_id
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        volume = arguments[ACTION_PARAM_RESOURCES]
        volume_id = volume["VolumeId"]
        account = volume["AwsAccount"]
        region = volume["Region"]
        return "{}-{}-{}-{}".format(account, region, volume_id, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "create_snapshot",
                "describe_tags",
                "modify_snapshot_attribute",
                "create_tags",
                "delete_tags"
            ]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods,
                                                       region=self.volume["Region"],
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    def create_volume_snapshot(self):

        def create_snapshot(snapshot_description):
            snapshot_id = ""
            try:
                create_snapshot_resp = self.ec2_client.create_snapshot_with_retries(DryRun=self.dryrun,
                                                                                    VolumeId=self.volume_id,
                                                                                    Description=snapshot_description)
                snapshot_id = create_snapshot_resp["SnapshotId"]
                self._logger_.info(INFO_SNAPSHOT_CREATED, snapshot_id)
                return snapshot_id

            except Exception as ex:
                if self.dryrun:
                    self._logger_.info(str(ex))
                else:
                    raise ex

            return snapshot_id

        def set_snapshot_tags(snap):
            try:
                tags = get_tags_for_volume_snapshot()

                if self.set_snapshot_name:

                    snapshot_name = self.build_str_from_template(parameter_name=PARAM_NAME,
                                                                 tag_variables={
                                                                     TAG_PLACEHOLDER_VOLUME_ID: self.volume_id})
                    if snapshot_name == "":
                        dt = self._datetime_.utcnow()
                        snapshot_name = SNAPSHOT_NAME.format(self.volume_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

                    prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                                          tag_variables={
                                                              TAG_PLACEHOLDER_VOLUME_ID: self.volume_id
                                                          })
                    snapshot_name = prefix + snapshot_name
                    tags["Name"] = snapshot_name

                    tags[actions.marker_snapshot_tag_source_source_volume_id()] = self.volume_id

                    self._logger_.info(INFO_SNAPSHOT_NAME, snapshot_name)

                if len(tags) > 0:
                    self._logger_.info(INFO_CREATE_TAGS, tags)
                    tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                         resource_ids=[snap],
                                         tags=tags,
                                         can_delete=False,
                                         logger=self._logger_)

                    self.result["tags"] = tags
                    self._logger_.info(INFO_TAGS_CREATED)

                return
            except Exception as ex:
                if self.dryrun:
                    self._logger_.debug(str(ex))
                else:
                    raise ex

        def get_tags_for_volume_snapshot():

            vol_tags = (self.copied_volume_tagfilter.pairs_matching_any_filter(self.volume.get("Tags", {})))
            vol_tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_VOLUME_ID: self.volume_id
                                              }))

            return vol_tags

        description = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_DESCRIPTION,
                                                   tag_variables={
                                                       TAG_PLACEHOLDER_VOLUME_ID: self.volume_id
                                                   })
        if description == "":
            description = SNAPSHOT_DESCRIPTION.format(self._task_, self.volume_id)

        self._logger_.info(INFO_CREATE_SNAPSHOT, self.volume_id)

        snapshot = create_snapshot(description)
        set_snapshot_tags(snapshot)
        return snapshot

    def is_completed(self, snapshot_create_data):

        def grant_create_volume_permissions(snap_id):

            if self.accounts_with_create_permissions is not None and len(self.accounts_with_create_permissions) > 0:

                args = {"CreateVolumePermission": {
                    "Add": [{"UserId": a.strip()} for a in self.accounts_with_create_permissions]
                }, "SnapshotId": snap_id}

                try:
                    self.ec2_client.modify_snapshot_attribute_with_retries(**args)
                    self._logger_.info(INFO_SETTING_CREATE_VOLUME_PERMISSIONS, ", ".join(self.accounts_with_create_permissions))
                    self.result["create-volume-access-accounts"] = [a.strip() for a in self.accounts_with_create_permissions]
                except Exception as ex:
                    raise_exception(ERR_SETTING_CREATE_VOLUME_PERMISSIONS, self.accounts_with_create_permissions, ex)

        def tag_shared_snapshot(snap_id, tags):

            for account in self.accounts_with_create_permissions:

                session_for_tagging = self.get_action_session(account=account,
                                                              param_name=PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME,
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
                                                         region=self._region_,
                                                         session=session_for_tagging,
                                                         logger=self._logger_)

                    tagging.set_ec2_tags(ec2_client=ec2_client,
                                         resource_ids=[snap_id],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INFO_SET_SNAPSHOT_TAGS_SHARED, safe_json(tags, indent=3), snap_id, account)

                except Exception as ex:
                    raise_exception(ERR_SETTING_SHARED_TAGS, account, str(ex))

        def set_volume_tags(snap_id):
            tags = self.build_tags_from_template(parameter_name=PARAM_VOLUME_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_SNAPSHOT: snap_id
                                                 })
            if len(tags) > 0:
                try:
                    tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                         resource_ids=[self.volume_id],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INFO_SET_VOLUME_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                       self.volume_id)
                except Exception as ex:
                    raise_exception(ERR_SETTING_VOLUME_TAGS, self.volume_id, ex)

        self._logger_.debug("Start result data is {}", safe_json(snapshot_create_data, indent=3))

        snapshot_id = snapshot_create_data.get("snapshot-id")

        self._logger_.info(INFO_CHECKING_STATUS, snapshot_id)

        # create service to test is snapshot is available
        ec2 = services.create_service("ec2",
                                      session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        snapshot = ec2.get(services.ec2_service.SNAPSHOTS,
                           OwnerIds=["self"],
                           region=self.volume["Region"],
                           SnapshotIds=[snapshot_id])

        if snapshot is None:
            self._logger_.info(INFO_NOT_CREATED_YET)
            return None

        test_result = {
            "VolumeId": snapshot["VolumeId"],
            "SnapshotId": snapshot["SnapshotId"],
            "State": snapshot["State"],
            "Progress": snapshot["Progress"]
        }

        self._logger_.info(INFO_STATE_SNAPSHOT, safe_json(test_result, indent=3))

        if snapshot["State"] == SNAPSHOT_STATE_PENDING:
            self._logger_.info(INFO_CREATION_PENDING)
            return None

        if snapshot["State"] == SNAPSHOT_STATE_ERROR:
            s = ERR_FAILED_SNAPSHOT.format(snapshot_id, self.volume_id)
            self._logger_.error(s)
            raise Exception(s)

        # set tags on source volume
        set_volume_tags(snapshot_id)
        # set permissions to create volumes from snapshots
        grant_create_volume_permissions(snapshot_id)
        # tag resources in accounts the snapshots are shred with
        tag_shared_snapshot(snapshot_id, snapshot.get("Tags", {}))

        self._logger_.info(INFO_COMPLETED)
        return test_result

    def execute(self):

        def volume_has_active_snapshots(vol_id):
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

            # test if the snapshot with the ids that were returned from the CreateSnapshot API call exists and are completed
            volume_snapshots = list(ec2.describe(services.ec2_service.SNAPSHOTS,
                                                 OwnerIds=["self"],
                                                 region=self._region_,
                                                 Filters=[
                                                     {
                                                         "Name": "volume-id", "Values": [vol_id]
                                                     }
                                                 ]))

            active = [s["SnapshotId"] for s in volume_snapshots if s.get("State", "") == "pending"]

            if len(active) > 0:
                self._logger_.info(INFO_PENDING_SNAPSHOTS, vol_id, ",".join(active))
                return True

            return False

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INFO_START_SNAPSHOT_ACTION, self.volume_id, self._account_, self._region_, self._task_)

        if volume_has_active_snapshots(self.volume_id):
            raise_exception(ERR_SNAPSHOT_PENDING, self.volume_id)

        self.result["snapshot-id"] = self.create_volume_snapshot()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CreatedSnapshots=1)

        return self.result