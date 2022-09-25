import logging
from argparse import ArgumentParser
from typing import Any, Collection

from django.conf import settings
from django.core.management.base import CommandError
from django.db import transaction

from zerver.lib.logging_util import log_to_file
from zerver.lib.management import ZulipBaseCommand
from zerver.models import UserProfile, Stream, Recipient
import json

## Setup ##
logger = logging.getLogger("zulip.import_rocketchat_channel_mapping")
log_to_file(logger, settings.LDAP_SYNC_LOG_PATH)

# Run this on a cron job to pick up on name changes.
@transaction.atomic
def import_rocketchat_channel_mapping(
    realm, remove_obsolete_membership, remove_all_individual_membership, channel_mapping
) -> None:
    logger.info("Starting update, using realm %s", realm.name)
    try:
        if remove_all_individual_membership:
            all_channel_set=set().union(*(channel_mapping.items()))

            for channel in all_channel_set:
                stream_query = Stream.objects.filter(realm=realm,name=channel)
                if not stream_query.exists():
                    logger.warn("Cannot find stream %s, skipping", channel)
                    continue

                stream = stream_query.get()

                Recipient.objects.filter(
                    type=Recipient.STREAM,
                    type_id=stream.id,
                ).delete()

        for ldap_group, channel_list in channel_mapping.items():
            for channel in channel_list:
                stream_query = Stream.objects.filter(realm=realm,name=channel)
                if not stream_query.exists():
                    logger.warn("Cannot find stream %s, skipping", channel)
                    continue

                stream = stream_query.get()

                #


    except Exception:
        logger.error("LDAP sync failed", exc_info=True)
        raise

    logger.info("Finished update.")


class Command(ZulipBaseCommand):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--remove-obsolete-membership",
            dest="remove_obsolete_membership",
            action="store_true",
            help="Remove individual channel membership if membership covered by group.",
        )

        parser.add_argument(
            "--remove-all-individual-membership",
            dest="remove_all_individual_membership",
            action="store_true",
            help="Remove all individual membership, only group memberships will be left.",
        )

        self.add_realm_args(parser)

        parser.add_argument(
            "rocketchat_mapping_file",
            metavar="<rocketchat mapping file>",
            help="A json file containing the rocketchat ldap to channel mapping",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        realm = self.get_realm(options)

        with open(options["rocketchat_mapping_file"], "r") as infile:
            channel_mapping = json.loads(infile.read())
            import_rocketchat_channel_mapping(
                realm,
                remove_obsolete_membership=options["remove_obsolete_membership"],
                remove_all_individual_membership=options["remove_all_individual_membership"],
                channel_mapping=channel_mapping,
            )
