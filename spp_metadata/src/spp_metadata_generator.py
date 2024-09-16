import os
from qiimp import  \
    HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, SAMPLE_TYPE_KEY, \
    extract_config_dict, write_quiet_extended_metadata_from_df
from spp_metadata.src.spp_literals import *


def write_extended_spp_metadata(input_metadata_df, output_dir, output_base,
                                study_specific_transformers_dict=None):
    spp_config_dict = extract_config_dict(None, starting_fp=__file__)
    spp_extendable_metadata_df = \
        _standardize_spp_input_metadata_df(input_metadata_df)

    output_base, output_ext = os.path.splitext(output_base)
    write_quiet_extended_metadata_from_df(
        spp_extendable_metadata_df, spp_config_dict, output_dir, output_base,
        output_ext, sep="\t", suppress_empty_fails=True, use_timestamp=False,
        study_specific_transformers_dict=study_specific_transformers_dict)


def _standardize_spp_input_metadata_df(input_metadata_df):
    input_metadata_df[HOSTTYPE_SHORTHAND_KEY] = BLANK_HOSTTYPE_SHORTHAND_KEY
    input_metadata_df.loc[SAMPLETYPE_SHORTHAND_KEY] = \
        input_metadata_df[SAMPLE_TYPE_KEY].str.lower()

    return input_metadata_df
