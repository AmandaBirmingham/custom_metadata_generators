import os
import pandas
from qiimp.src.metadata_extender import \
    HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, SAMPLE_TYPE_KEY, \
    extract_config_dict, generate_extended_metadata_file_from_raw_metadata_df
from nph_metadata.src.nph_literals import *
from nph_metadata.src.nph_transformers import format_real_vs_blanks_dates

CORE_SAMPLE_NAME_KEY = "sample_name"
CORE_SAMPLE_ID_KEY = "mayo_sample_id"
MANIFEST_SAMPLE_ID_KEY = "sample_id"
TUBE_CODE_KEY = "TubeCode"
TUBE_ID_KEY = "tube_id"
MANIFEST_FILE_NAME_KEY = "manifest"
MERGE_KEY = "_merge"
BLANK_SUBSTRING = "BLANK"
MATRIX_ID_KEY = "matrix_id"
ADDITIVE_TREATMENT_KEY = "additive_treatment"
DNA_RNA_SHIELD_VALUE = "DNA/RNA Shield"


def make_nph_extendable_metadata_df(
        core_file_fp, manifests_dir, extraction_yyyy_mm):
    # load the samples info from the core file
    samples_df = pandas.read_csv(core_file_fp)
    # rename the TUBE_CODE_KEY column to TUBE_ID_KEY
    samples_df.rename(columns={TUBE_CODE_KEY: TUBE_ID_KEY}, inplace=True)
    samples_df[CORE_SAMPLE_ID_KEY] = \
        samples_df[CORE_SAMPLE_ID_KEY].astype("string")

    # keep loading manifests in reverse chronological order until all samples
    # in the samples_df have been matched to a manifest
    merged_df = _lazy_load_manifests(samples_df, manifests_dir)

    extendable_metadata_df = _standardize_nph_input_metadata_df(
        merged_df, extraction_yyyy_mm)
    return extendable_metadata_df


def _lazy_load_manifests(samples_df, manifests_dir):
    aggregated_manifests_df = None
    merged_df = None
    missing_samples = samples_df[CORE_SAMPLE_NAME_KEY].tolist()
    manifests_found = False

    for curr_manifest_fp in _get_fps_youngest_to_oldest(manifests_dir):
        manifests_found = True

        # get the file name from the current manifest file path
        curr_manifest_fn = os.path.basename(curr_manifest_fp)

        curr_manifest_df = pandas.read_csv(curr_manifest_fp, sep="\t")
        curr_manifest_df[MANIFEST_FILE_NAME_KEY] = curr_manifest_fn
        curr_manifest_df[MANIFEST_SAMPLE_ID_KEY] = \
            curr_manifest_df[MANIFEST_SAMPLE_ID_KEY].astype("string")
        if aggregated_manifests_df is None:
            aggregated_manifests_df = curr_manifest_df
        else:
            aggregated_manifests_df = pandas.concat(
                [aggregated_manifests_df, curr_manifest_df])
        # end if this is/isn't first manifest checked

        # merge the aggregated manifest with the samples_df
        merged_df = samples_df.merge(aggregated_manifests_df,
                                     how="left", indicator=True,
                                     left_on=CORE_SAMPLE_ID_KEY,
                                     right_on=MANIFEST_SAMPLE_ID_KEY)

        # if all samples that aren't blanks in the sample_df had a match in the
        # aggregated manifests, break out of the manifest-loading loop and
        # return.  If there are still non-blank samples that occur only in the
        # samples_df, abandon this merge and go aggregate the next manifest
        missing_samples_mask = \
            (merged_df[MERGE_KEY] == "left_only") & \
            (~merged_df[CORE_SAMPLE_ID_KEY].str.contains(BLANK_SUBSTRING))
        # get the sample_names for the rows in the missing_samples_mask
        missing_samples = \
            merged_df.loc[missing_samples_mask, CORE_SAMPLE_NAME_KEY].tolist()
        if len(missing_samples) == 0:
            break
    # end for each manifest

    if not manifests_found:
        raise ValueError(
            f"No manifests found in the directory '{manifests_dir}'")
    if len(missing_samples) > 0:
        raise ValueError(
            f"These samples were not found in any manifest: {missing_samples}")

    # remove the MERGE_KEY column from the merged_df before returning
    merged_df.drop(columns=[MERGE_KEY], inplace=True)
    return merged_df


def _get_fps_youngest_to_oldest(inputs_dir):
    # get all the files that are immediate children of inputs_dir
    file_paths = [os.path.join(inputs_dir, x) for x in os.listdir(inputs_dir)
                  if os.path.isfile(os.path.join(inputs_dir, x))]
    file_paths.sort(key=os.path.getmtime, reverse=True)

    # return file paths in reverse chronological order, with the
    # latest-modified file first
    for curr_file_path in file_paths:
        yield curr_file_path


def _standardize_nph_input_metadata_df(input_metadata_df, extraction_yyyy_mm):
    is_blank_mask = \
        input_metadata_df[CORE_SAMPLE_NAME_KEY].str.contains(BLANK_SUBSTRING)
    input_metadata_df.loc[is_blank_mask, MATRIX_ID_KEY] = \
        input_metadata_df.loc[is_blank_mask, TUBE_ID_KEY]

    input_metadata_df.loc[is_blank_mask, COLLECTION_DATE_TIME_KEY] = \
        extraction_yyyy_mm

    input_metadata_df.loc[is_blank_mask, ADDITIVE_TREATMENT_KEY] = \
        DNA_RNA_SHIELD_VALUE

    input_metadata_df.loc[is_blank_mask, HOSTTYPE_SHORTHAND_KEY] = \
        "sterile_water_blank"
    input_metadata_df.loc[~is_blank_mask, HOSTTYPE_SHORTHAND_KEY] = "human"

    input_metadata_df.loc[is_blank_mask, SAMPLETYPE_SHORTHAND_KEY] = \
        "control shield"
    input_metadata_df.loc[~is_blank_mask, SAMPLETYPE_SHORTHAND_KEY] = \
        input_metadata_df[SAMPLE_TYPE_KEY].str.lower()
    return input_metadata_df


if __name__ == "__main__":
    # TODO: remove hardcoded arguments
    # raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_raw_metadata_short.xlsx"
    # raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_nph_metadata_short.xlsx"
    core_file_fp = "/Users/abirmingham/Desktop/metadata/mod_NPH_011 Sample Processing spreadsheet_SAS KL.csv"
    manifests_dir = "/Users/abirmingham/Desktop/metadata/manifests"
    extraction_yyyy_mm = "2023-12"
    output_dir = "/Users/abirmingham/Desktop/"
    output_base = "test_extended_metadata"

    nph_config_dict = extract_config_dict(None, starting_fp=__file__)
    nph_extendable_metadata_df = make_nph_extendable_metadata_df(
        core_file_fp, manifests_dir, extraction_yyyy_mm)

    nph_transformers_dict = \
        {"format_real_vs_blanks_dates": format_real_vs_blanks_dates}

    generate_extended_metadata_file_from_raw_metadata_df(
        nph_extendable_metadata_df, nph_config_dict, output_dir, output_base,
        study_specific_transformers_dict=nph_transformers_dict)
