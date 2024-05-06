from qiimp.src.metadata_extender import HOSTTYPE_SHORTHAND_KEY
from qiimp.src.metadata_transformers import format_a_datetime
from nph_metadata.src.nph_literals import *


def format_real_vs_blanks_dates(row, source_fields):
    if row[HOSTTYPE_SHORTHAND_KEY] == BLANK_HOSTTYPE_SHORTHAND_KEY:
        return row[COLLECTION_DATE_TIME_KEY]
    else:
        return format_a_datetime(row, source_fields)
