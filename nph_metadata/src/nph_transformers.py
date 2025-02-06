from qiimp import HOSTTYPE_SHORTHAND_KEY
from qiimp import transform_date_to_formatted_date
from nph_metadata.src.nph_literals import *


def format_real_vs_blanks_dates(row, source_fields):
    if row[HOSTTYPE_SHORTHAND_KEY] == BLANK_HOSTTYPE_SHORTHAND_KEY:
        return row[COLLECTION_DATE_TIME_KEY]
    else:
        return transform_date_to_formatted_date(row, source_fields)
