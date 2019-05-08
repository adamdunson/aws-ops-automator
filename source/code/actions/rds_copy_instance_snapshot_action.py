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

import handlers.rds_event_handler
import services.kms_service
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from outputs import raise_exception, raise_value_error
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

ERR_LISTING_KEYS_IN_DESTINATION = "Error listing keys for account {} in region {}, {}"

SNAPSHOT_STATE_FAILED = "failed"
SNAPSHOT_STATE_CREATING = "creating"
SNAPSHOT_STATE_AVAILABLE = "available"

KMS_KEY_PATTERN = r"arn:aws-us-gov:kms:(.)*:key\/([0-9,a-f]){8}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){12}"
KMS_ALIAS_PATTERN = r"arn:aws-us-gov:alias:(.)*:key\/([0-9,a-f]){8}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){12}"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

COPIED_SNAPSHOTS_BOTH = "Both"
COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT = "SharedToAccount"
COPIED_OWNED_BY_ACCOUNT = "OwnedByAccount"

TAG_PLACEHOLDER_SOURCE_REGION = "source-region"
TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID = "source-snapshot-id"
TAG_PLACEHOLDER_OWNER_ACCOUNT = "owner-account"
TAG_PLACEHOLDER_DESTINATION_REGION = "destination-region"
TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID = "copy-snapshot-id"

MARKER_TAG_SOURCE_SNAPSHOT_ARN = "OpsAutomator:{}-RdsCopyInstanceSnapshot-CopiedFromSnapshot"
MARKER_TAG_COPIED_TO = "OpsAutomator:{}-{}-RdsCopyInstanceSnapshot-CopiedTo"

GROUP_LABEL_SNAPSHOT_COPY_OPTIONS = "Snapshot copy options"
GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS = "Permissions and encryption"

PARAM_DESC_SOURCE_ACCOUNT_TAG_ROLENAME = \
    "Name of the cross account role in the accounts that own a shared snapshot, that is used to read and create tags in these " \
    "accounts. Leave this parameter empty to use the default role with name \"{}\". The role must give permissions to use the " \
    "RdsSetTags action.".format(handlers.default_rolename_for_stack())
PARAM_DESC_COPIED_SNAPSHOT_TAGS = "Copied tags from source snapshot"
PARAM_DESC_SNAPSHOT_NAME = "Name of the copied snapshot, leave blank for default snapshot name"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for name of created snapshots."
PARAM_DESC_DESTINATION_REGION = "Destination region for copied snapshot"
PARAM_DESC_COPIED_SNAPSHOTS = \
    "Select which snapshots are copied. Snapshots owned by the account, shared with the account or both. Note that for " \
    "shared snapshots the tags are initially empty so the tagfilter must be set to '*' to automatically copy these shared " \
    "snapshots "
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to copied snapshot. Note that tag values for RDS cannot contain ',' characters. When specifying multiple " \
    "follow up tasks in the value of the Ops Automator task list tag use a '/' character instead of a ','"
PARAM_DESC_ACCOUNTS_RESTORE_PERMISSIONS = "List of account that will be granted access to restore RDS instances from the copied " \
                                          "snapshot."
PARAM_DESC_KMS_KEY = \
    "The AWS KMS key for an encrypted DB snapshot. The KMS key ID is the Amazon Resource Name (ARN), KMS key identifier, " \
    "or the KMS key alias for the KMS encryption key. If you copy an encrypted DB snapshot from your AWS account, you can " \
    "specify a value for this parameter to encrypt the copy with a new KMS encryption key. If you don't specify a value for this " \
    "parameter, then the copy of the DB snapshot is encrypted with the same KMS key as the source DB snapshot. If you copy an " \
    "encrypted DB snapshot that is shared from another AWS account, then you must specify a value for this parameter. " \
    "If you specify this parameter when you copy an unencrypted snapshot, the copy is encrypted. If you copy an encrypted " \
    "snapshot to a different AWS Region, then you must specify a KMS key for the destination AWS Region. KMS encryption keys " \
    "are specific to the AWS Region that they are created in, and you cannot use encryption keys from one AWS Region in " \
    "another AWS Region. The role or account executing this task must have access to use the specified KMS key."
PARAM_DESC_COPY_SHARED_FROM_ACCOUNTS = \
    "Comma separated list of accounts to copy shared snapshots from. Leave blank to copy shared snapshots from all accounts"
PARAM_DESC_SOURCE_SNAPSHOT_TAGS = "Tags to set on source snapshot after successful copy to destination"

PARAM_LABEL_SOURCE_ACCOUNT_TAG_ROLENAME = "Cross account roles for tagging source snapshots in account sharing the snapshot"
PARAM_LABEL_COPIED_SNAPSHOT_TAGS = "Copied tags"
PARAM_LABEL_DESTINATION_REGION = "Destination region"
PARAM_LABEL_COPIED_SNAPSHOTS = "Snapshots copied"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL__ACCOUNTS_RESTORE_PERMISSIONS = "Accounts with restore permissions"
PARAM_LABEL_COPY_SHARED_FROM_ACCOUNTS = "Shared by accounts"
PARAM_LABEL_KMS_KEY = "KMS Key"
PARAM_LABEL_SNAPSHOT_NAME = "Snapshot name"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SOURCE_SNAPSHOT_TAGS = "Source snapshot tags"

PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME = "TagOwnerAccountRoleName"
PARAM_ACCOUNTS_RESTORE_PERMISSIONS = "RestorePermission"
PARAM_COPIED_SNAPSHOT_TAGS = "CopiedSnapshotTags"
PARAM_COPIED_SNAPSHOTS = "SourceSnapshotTypes"
PARAM_COPY_SHARED_FROM_ACCOUNTS = "CopiedSharedFromAccounts"
PARAM_DESTINATION_REGION = "DestinationRegion"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_SOURCE_SNAPSHOT_TAGS = "SourceSnapshotTags"
PARAM_KMS_KEY = "KmsKeyArn"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_NAME = "SnapshotName"

INFO_ACCOUNT_SNAPSHOT = "Copying RDS instance snapshot {} for account {} from region {} to region {}"
INFO_CHECK_COMPLETED_RESULT = "RDS snapshot copy completion check result is {}"
INFO_COPY_COMPLETED = "RDS snapshot {} from region {} copied to snapshot {} in region"
INFO_COPY_PENDING = "RDS snapshot with id {} does not exist or is pending in region {}"
INFO_CREATE_TAGS = "Creating tags {} for copied RDS snapshot"
INFO_SETTING_CREATE_SNAPSHOT_PERMISSIONS = "Setting restore snapshot permissions for {}"
INFO_SNAPSHOT_COPIED = "Copy of  RDS snapshot {} to region {} snapshot {} started"
INFO_TAGS_CREATED = "Tags created for copied RDS snapshot {}"
INFO_CREATE_SOURCE_TAGS = "Creating tags {} for source snapshot {}"

ERR_ACCOUNTS_BUT_NOT_SHARED = "Parameter {} can only be used if {} parameter has been set to copy shared snapshots"
ERR_INVALID_DESTINATION_REGION = "{} is not a valid region, valid regions are: "
ERR_KMS_KEY_NOT_IN_REGION = "KMS key with id {} is not available in destination region {}"
ERR_KMS_KEY_ONLY_IF_ENCRYPTED = "{} parameter can only be used if encryption is enabled"
ERR_SETTING_RESTORE_PERMISSIONS = "Error setting restore permissions for account(s) {}, {}"
ERR_COPYING_RDS_SNAPSHOT = "Error copying RDS snapshot"
ERR_GRANTING_PERMISSIONS = "Error granting restore permissions to copied snapshot for accounts {}"
ERR_NO_ACCOUNTS_SPECIFIED_TO_COPY_SHARED_SNAPSHOTS_FROM = \
    "If snapshots shared from other accounts are copied a comma separated list of accounts of these accounts must be specified"
ERR_TAGS_NOT_SET_FOR_ARN = "Tags not set for source snapshot with ARN {}"
ERR_KMS_KEY_NOT_EXIST_OR_NOT_ENABLED = "KMS key {} is not available or os not enabled in destination region {} for account {}"

DEBUG_ONLY_COPY_OWNED_SNAPSHOTS = \
    "RDS snapshot {} is owned by account {}, because option {} is set to only copy snapshots owned by account {} it is not " \
    "selected"
DEBUG_ONLY_COPY_SHARED_SNAPSHOTS = \
    "RDS snapshot {} is owned by account {}, because option {} is set to only copy snapshots shared to account {} it is not " \
    "selected"
DEBUG_SHARED_SNAPSHOT_OWNER_NOT_IN_LIST = \
    "RDS snapshot {} shared by account {} is not copied as it is not in the list of accounts to copy snapshots from {}"
DEBUG_SNAPSHOT_ALREADY_COPIED = \
    "RDS snapshot {} not selected as it already has been copied as snapshot {} in destination region {},"
ERR_CAN_NOT_SHARE_ENCRYPTED_DEFAULT_KEY = \
    "Snapshot {} can not be shared as it is encrypted with the default RDS key"


def build_select_parameters(task, parameters):
    copied_snapshots = parameters.get(PARAM_COPIED_SNAPSHOTS, "")

    copying_shared = parameters.get(PARAM_COPIED_SNAPSHOTS, "") in [COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT, COPIED_SNAPSHOTS_BOTH]

    if copied_snapshots == COPIED_OWNED_BY_ACCOUNT:
        select_parameters = {"SnapshotType": "manual"}
    elif copied_snapshots == COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT:
        select_parameters = {
            "SnapshotType": "shared",
            "IncludeShared": True
        }
    else:
        select_parameters = {"IncludeShared": True}

    if copying_shared:
        tag_select_roles = []
        copied_from_accounts = parameters.get(PARAM_COPY_SHARED_FROM_ACCOUNTS, [])

        for account in copied_from_accounts:
            role = handlers.get_account_role(account=account, task=task, param_name=PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME)
            if role is not None:
                tag_select_roles.append(role)

        if len(tag_select_roles) != 0:
            select_parameters["tag_roles"] = tag_select_roles

    return select_parameters


class RdsCopyInstanceSnapshotAction(ActionBase):
    """
    Class implements action for copying EC2 Snapshots
    """

    properties = {
        ACTION_TITLE: "RDS Copy Instance Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Copies RDS instance snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "10398d72-e6bf-4167-9253-93b47deff098",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_EXPRESSION:
            "DBSnapshots[?Status=='available'].{"
            "DBSnapshotIdentifier:DBSnapshotIdentifier, "
            "DBInstanceIdentifier:DBInstanceIdentifier, "
            "DBSnapshotArn:DBSnapshotArn, "
            "SnapshotType:SnapshotType, "
            "Encrypted:Encrypted, "
            "KmsKeyId:KmsKeyId, "
            "Status:Status}",

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: build_select_parameters,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_COMPLETION_TIMEOUT_MINUTES: 180,

        ACTION_MIN_INTERVAL_MIN: 15,

        # RDS only allows 5 concurrent copies per account to a destination region
        ACTION_MAX_CONCURRENCY: int(os.getenv(handlers.ENV_SERVICE_LIMIT_CONCURRENT_RDS_SNAPSHOT_COPY, 5)),

        ACTION_PARAMETERS: {
            PARAM_DESTINATION_REGION: {
                PARAM_DESCRIPTION: PARAM_DESC_DESTINATION_REGION,
                PARAM_LABEL: PARAM_LABEL_DESTINATION_REGION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: services.get_session().region_name,
                PARAM_ALLOWED_VALUES: [str(r) for r in services.get_session().get_available_regions("rds", "aws-us-gov")]
            },
            PARAM_SNAPSHOT_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME_PREFIX
            },
            PARAM_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME
            },
            PARAM_COPIED_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_COPIED_SNAPSHOT_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_ACCOUNT_TAG_ROLENAME,
                PARAM_LABEL: PARAM_LABEL_SOURCE_ACCOUNT_TAG_ROLENAME,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_COPIED_SNAPSHOTS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_SNAPSHOTS,
                PARAM_LABEL: PARAM_LABEL_COPIED_SNAPSHOTS,
                PARAM_TYPE: str,
                PARAM_ALLOWED_VALUES: [COPIED_OWNED_BY_ACCOUNT, COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT, COPIED_SNAPSHOTS_BOTH],
                PARAM_DEFAULT: COPIED_OWNED_BY_ACCOUNT,
                PARAM_REQUIRED: False
            },
            PARAM_SOURCE_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_SOURCE_SNAPSHOT_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_ACCOUNTS_RESTORE_PERMISSIONS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_RESTORE_PERMISSIONS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL__ACCOUNTS_RESTORE_PERMISSIONS
            },
            PARAM_COPY_SHARED_FROM_ACCOUNTS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPY_SHARED_FROM_ACCOUNTS,
                PARAM_LABEL: PARAM_LABEL_COPY_SHARED_FROM_ACCOUNTS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False
            },
            PARAM_KMS_KEY: {
                PARAM_DESCRIPTION: PARAM_DESC_KMS_KEY,
                PARAM_LABEL: PARAM_LABEL_KMS_KEY,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_SNAPSHOT_COPY_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_DESTINATION_REGION,
                    PARAM_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_COPIED_SNAPSHOTS,
                    PARAM_COPY_SHARED_FROM_ACCOUNTS,
                    PARAM_COPIED_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_SOURCE_SNAPSHOT_TAGS,
                    PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_KMS_KEY,
                    PARAM_ACCOUNTS_RESTORE_PERMISSIONS
                ],
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:CopyDBSnapshot",
            "rds:AddTagsToResource",
            "rds:RemoveTagsFromResource",
            "rds:DescribeDBSnapshots",
            "rds:ListTagsForResource",
            "rds:ModifyDBsnapshotAttribute",
            "kms:DescribeKey",
            "tag:GetResources"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_snapshot = self._resources_

        # snapshot source and destination information
        self.source_snapshot_id = self.db_snapshot["DBSnapshotIdentifier"]
        self.source_snapshot_arn = self.db_snapshot["DBSnapshotArn"]

        self.source_region = self.db_snapshot["Region"]
        self.destination_region = self.get(PARAM_DESTINATION_REGION)

        self.kms_key_id = self.get(PARAM_KMS_KEY, None)
        self.copied_snapshots = self.get(PARAM_COPIED_SNAPSHOTS)
        self.accounts_with_restore_permissions = self.get(PARAM_ACCOUNTS_RESTORE_PERMISSIONS, [])

        self._kms_service = None

        # tagging
        self.copied_snapshot_tagfilter = TagFilterSet(self.get(PARAM_COPIED_SNAPSHOT_TAGS, ""))

        source_db_instance = self.db_snapshot.get("DBInstanceIdentifier", None)
        if source_db_instance is None:
            source_db_instance_from_tag = self.db_snapshot.get("Tags", {}).get(
                MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID.format(os.getenv(handlers.ENV_STACK_NAME)), None)
            if source_db_instance_from_tag is not None:
                source_db_instance = source_db_instance_from_tag
        self.source_volume_id = source_db_instance

        self._rds_destination_client = None

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "destination-region": self.destination_region,
            "source-region": self.source_region,
            "source-snapshot-id": self.source_snapshot_id
        }

    @property
    def kms_service(self):
        if self._kms_service is None:
            self._kms_service = services.create_service("kms", session=self._session_,
                                                        service_retry_strategy=get_default_retry_strategy("kms",
                                                                                                          context=self._context_))
        return self._kms_service

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        # source snapshot
        db_snapshot_arn = resource["DBSnapshotArn"]
        parameters = task.get(handlers.TASK_PARAMETERS, {})

        copied_tag_name = MARKER_TAG_COPIED_TO.format(os.getenv(handlers.ENV_STACK_NAME), task[handlers.TASK_NAME])[0:128]
        if copied_tag_name in resource.get("Tags", {}):
            region, snapshot = resource["Tags"][copied_tag_name].split(":", 1)
            if region == parameters[PARAM_DESTINATION_REGION]:
                logger.debug(DEBUG_SNAPSHOT_ALREADY_COPIED, db_snapshot_arn, snapshot, parameters[PARAM_DESTINATION_REGION])
                return None

        copied_snapshots = parameters[PARAM_COPIED_SNAPSHOTS]
        snapshot_type = resource.get("SnapshotType", "")

        if snapshot_type not in ["manual", "shared"]:
            logger.debug("Snapshot {} of type {} skipped", db_snapshot_arn, snapshot_type)
            return None

        if copied_snapshots == COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT and snapshot_type != "shared":
            logger.debug("owned snapshot {} skipped as only shared snapshots are selected", db_snapshot_arn)
            return None

        if copied_snapshots == COPIED_OWNED_BY_ACCOUNT and snapshot_type != "manual":
            logger.debug("shared snapshot {} skipped as only owned snapshots are selected", db_snapshot_arn)
            return None

        snapshot_owner = db_snapshot_arn.split(":")[4]
        account = resource["AwsAccount"]

        # copy owned, shared or both

        copy_from_accounts = [a.strip() for a in parameters.get(PARAM_COPY_SHARED_FROM_ACCOUNTS, [])]

        if snapshot_owner != account and snapshot_owner not in copy_from_accounts:
            logger.debug(DEBUG_SHARED_SNAPSHOT_OWNER_NOT_IN_LIST, db_snapshot_arn, snapshot_owner, ",".join(copy_from_accounts))
            return None

        return resource

    def get_kms_key(self, keyid):
        try:
            key = self.kms_service.get(services.kms_service.KEY,
                                       region=self.destination_region,
                                       KeyId=keyid)
            return key
        except Exception as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code") == "NotFoundException":
                if not keyid.startswith("arn") and not keyid.startswith("alias/"):
                    return self.get_kms_key("alias/" + keyid)
                return None
            else:
                raise_exception(ERR_LISTING_KEYS_IN_DESTINATION, self._account_, self.destination_region, ex)

    @staticmethod
    def action_concurrency_key(arguments):
        # copies per account/destination
        return "ec2:RdsCopySnapshot:{}:{}".format(arguments[ACTION_PARAM_ACCOUNT], arguments[PARAM_DESTINATION_REGION])

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        valid_regions = services.get_session().get_available_regions("rds", "aws-us-gov")
        region = parameters.get(PARAM_DESTINATION_REGION)
        if region not in valid_regions:
            raise_value_error(ERR_INVALID_DESTINATION_REGION, region, ",".join(valid_regions))

        if parameters.get(PARAM_COPIED_SNAPSHOTS, "") != COPIED_OWNED_BY_ACCOUNT and \
                len(parameters.get(PARAM_COPY_SHARED_FROM_ACCOUNTS, [])) == 0:
            raise_value_error(ERR_NO_ACCOUNTS_SPECIFIED_TO_COPY_SHARED_SNAPSHOTS_FROM)

        if len(parameters.get(PARAM_COPY_SHARED_FROM_ACCOUNTS, [])) > 0 and \
                parameters[PARAM_COPIED_SNAPSHOTS] == COPIED_OWNED_BY_ACCOUNT:
            raise_value_error(ERR_ACCOUNTS_BUT_NOT_SHARED, PARAM_COPY_SHARED_FROM_ACCOUNTS, PARAM_COPIED_SNAPSHOTS)
        return parameters

    def is_completed(self, snapshot_create_data):

        def grant_restore_permissions(snapshot):

            if self.accounts_with_restore_permissions is not None and len(self.accounts_with_restore_permissions) > 0:

                if snapshot.get("Encrypted", False):
                    default_rds_key_arn = self.get_kms_key("alias/aws/rds").get("Arn")
                    if snapshot.get("KmsKeyId") == default_rds_key_arn:
                        self._logger_.error(ERR_CAN_NOT_SHARE_ENCRYPTED_DEFAULT_KEY, snapshot["DBSnapshotIdentifier"])
                        return

                args = {
                    "DBSnapshotIdentifier": snapshot["DBSnapshotIdentifier"],
                    "AttributeName": "restore",
                    "ValuesToAdd": [a.strip() for a in self.accounts_with_restore_permissions]
                }

                try:
                    self.rds_destination_client.modify_db_snapshot_attribute_with_retries(**args)
                    self._logger_.info("Granting restore permissions to accounts {}",
                                       ", ".join(self.accounts_with_restore_permissions))
                    self.result["restore-access-accounts"] = [a.strip() for a in self.accounts_with_restore_permissions]
                except Exception as e:
                    raise_exception(ERR_GRANTING_PERMISSIONS, self.accounts_with_restore_permissions, e)

        def set_source_snapshot_tags(copy_id):

            snapshot_tags = self.build_tags_from_template(parameter_name=PARAM_SOURCE_SNAPSHOT_TAGS,
                                                          region=self.source_region,
                                                          tag_variables={
                                                              TAG_PLACEHOLDER_DESTINATION_REGION: self.destination_region,
                                                              TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID: copy_id
                                                          })

            self._logger_.info(INFO_CREATE_SOURCE_TAGS, snapshot_tags, self.source_snapshot_id)

            if len(snapshot_tags) > 0:
                source_client = self.rds_source_client(self.source_snapshot_arn)
                if source_client is None:
                    self._logger_.error(ERR_TAGS_NOT_SET_FOR_ARN, self.source_snapshot_arn)
                    return
                tagging.set_rds_tags(source_client, tags=snapshot_tags,
                                     logger=self._logger_,
                                     resource_arns=[self.source_snapshot_arn])

        # create service instance to test if snapshot exists
        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))
        copied_snapshot_id = snapshot_create_data["copied-snapshot-id"]

        # test if the snapshot with the id that was returned from the CopySnapshot API call exists and is completed
        # noinspection PyBroadException
        try:
            copied_snapshot = rds.get(services.rds_service.DB_SNAPSHOTS,
                                      region=self.destination_region,
                                      DBSnapshotIdentifier=copied_snapshot_id,
                                      _expected_boto3_exceptions_=["DBSnapshotNotFoundFault"])
        except Exception as ex:
            if "DBSnapshotNotFoundFault" in ex.message:
                copied_snapshot = None
            else:
                raise ex

        if copied_snapshot is not None:
            self._logger_.debug(INFO_CHECK_COMPLETED_RESULT, copied_snapshot)

        state = copied_snapshot["Status"] if copied_snapshot is not None else None

        if copied_snapshot is None or state == SNAPSHOT_STATE_CREATING:
            self._logger_.info(INFO_COPY_PENDING, copied_snapshot_id, self.destination_region)
            return None

        if state == SNAPSHOT_STATE_FAILED:
            copied_tag_name = MARKER_TAG_COPIED_TO.format(os.getenv(handlers.ENV_STACK_NAME), self._task_)[0:128]
            client = self.rds_source_client(self.source_snapshot_arn)
            if client is not None:
                client.remove_tags_from_resource_with_retries(ResourceName=self.source_snapshot_arn, TagKeys=[copied_tag_name])
                raise_exception(ERR_COPYING_RDS_SNAPSHOT)

        if state == SNAPSHOT_STATE_AVAILABLE:
            self._logger_.info(INFO_COPY_COMPLETED, self.source_snapshot_id, self.source_region, copied_snapshot_id,
                               self.destination_region)
            set_source_snapshot_tags(copied_snapshot_id)
            grant_restore_permissions(copied_snapshot)
            return copied_snapshot

        return None

    @staticmethod
    def action_logging_subject(arguments, _):
        db_snapshot = arguments[ACTION_PARAM_RESOURCES]
        account = db_snapshot["AwsAccount"]
        snapshot_id = db_snapshot["DBSnapshotIdentifier"]
        region = db_snapshot["Region"]
        return "{}-{}-{}-{}".format(account, region, snapshot_id, log_stream_date())

    @property
    def rds_destination_client(self):
        if self._rds_destination_client is None:
            methods = [
                "copy_db_snapshot",
                "add_tags_to_resource",
                "remove_tags_from_resource",
                "modify_db_snapshot_attribute"
            ]
            self._rds_destination_client = get_client_with_retries("rds",
                                                                   methods=methods,
                                                                   region=self.destination_region,
                                                                   context=self._context_,
                                                                   session=self._session_,
                                                                   logger=self._logger_)
        return self._rds_destination_client

    def rds_source_client(self, arn):
        snapshot_owner_account = arn.split(":")[4]
        snapshot_source_region = arn.split(":")[3]
        session = self.get_action_session(account=snapshot_owner_account,
                                          param_name=PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME,
                                          logger=self._logger_)
        if session is None:
            return None

        return get_client_with_retries("rds",
                                       methods=[
                                           "add_tags_to_resource",
                                           "remove_tags_from_resource"
                                       ],
                                       region=snapshot_source_region,
                                       context=self._context_,
                                       session=session,
                                       logger=self._logger_)

    def execute(self):

        def get_tags_for_copied_rds_snapshot():

            snapshot_tags = (self.copied_snapshot_tagfilter.pairs_matching_any_filter(self.db_snapshot.get("Tags", {})))

            tag_variables = {
                TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: self.source_snapshot_id,
                TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_
            }

            snapshot_tags.update(self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                                               region=self.source_region,
                                                               tag_variables=tag_variables,
                                                               restricted_value_set=True))

            snapshot_tags[MARKER_TAG_SOURCE_SNAPSHOT_ARN.format(os.getenv(handlers.ENV_STACK_NAME))] = self.source_snapshot_arn
            snapshot_tags[MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID.format(os.getenv(handlers.ENV_STACK_NAME))] = self.source_volume_id

            return snapshot_tags

        # logged information
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])
        self._logger_.info(INFO_ACCOUNT_SNAPSHOT, self.source_snapshot_id, self._account_, self.source_region,
                           self.destination_region)
        self._logger_.debug("Snapshot : {}", self.db_snapshot)

        prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                              region=self.source_region,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: self.source_snapshot_id,
                                                  TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                  TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_
                                              })

        snapshot_name = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME,

                                                     account=self._account_,
                                                     region=self.source_region,
                                                     tag_variables={
                                                         TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: self.source_snapshot_id,
                                                         TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                         TAG_PLACEHOLDER_OWNER_ACCOUNT: self._account_
                                                     })
        if snapshot_name == "":
            dt = self._datetime_.utcnow()
            snapshot_name = SNAPSHOT_NAME.format(self.source_snapshot_id, dt.year, dt.month, dt.day, dt.hour,
                                                 dt.minute)

        snapshot_name = prefix + snapshot_name

        # setup argument for CopySnapshot call
        args = {
            "SourceRegion": self.source_region,
            "SourceDBSnapshotIdentifier": self.source_snapshot_arn,
            "TargetDBSnapshotIdentifier": snapshot_name
        }

        if self.kms_key_id is not None:
            key = self.get_kms_key(keyid=self.kms_key_id)
            if key is None or key.get("KeyState") != "Enabled":
                raise_value_error(ERR_KMS_KEY_NOT_EXIST_OR_NOT_ENABLED, self.kms_key_id, self.destination_region, self._account_)
            args["KmsKeyId"] = key["Arn"]

        # start the copy
        resp = self.rds_destination_client.copy_db_snapshot_with_retries(**args)

        # id of the copy
        copied_snapshot_id = resp.get("DBSnapshot", {}).get("DBSnapshotIdentifier")
        copied_snapshot_arn = resp.get("DBSnapshot", {}).get("DBSnapshotArn")
        self._logger_.info(INFO_SNAPSHOT_COPIED, self.source_snapshot_id, self.destination_region, copied_snapshot_id)
        self.result["copied-snapshot-id"] = copied_snapshot_id
        self.result["copied-snapshot-arn"] = copied_snapshot_arn

        # set tag on the source to avoid multiple copies
        copied_tag_name = MARKER_TAG_COPIED_TO.format(os.getenv(handlers.ENV_STACK_NAME), self._task_)[0:128]

        self.rds_source_client(self.source_snapshot_arn).add_tags_to_resource_with_retries(
            ResourceName=self.source_snapshot_arn,
            Tags=tag_key_value_list({
                copied_tag_name: "{}:{}".format(self.destination_region, copied_snapshot_id)
            }))

        # set tags on the copy
        tags = get_tags_for_copied_rds_snapshot()
        self._logger_.info(INFO_CREATE_TAGS, tags)

        if len(tags) > 0:
            tagging.set_rds_tags(rds_client=self.rds_destination_client,
                                 resource_arns=[copied_snapshot_arn],
                                 tags=tags,
                                 can_delete=False,
                                 logger=self._logger_)

            self._logger_.info(INFO_TAGS_CREATED, copied_snapshot_id)

        self.result[METRICS_DATA] = build_action_metrics(self, CopiedDBSnapshots=1)

        return self.result
