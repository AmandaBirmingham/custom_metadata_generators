from collections import defaultdict
from datetime import datetime
import os
import pandas
from qiimp import  \
    HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, SAMPLE_TYPE_KEY, \
    extract_config_dict, load_df_with_best_fit_encoding, \
    write_extended_metadata_from_df
from nph_metadata.src.nph_literals import *
from nph_metadata.src.nph_transformers import format_real_vs_blanks_dates

CORE_SAMPLE_NAME_KEY = "sample_name"
HOST_SUBJECT_ID_KEY = "host_subject_id"
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
    samples_df = pandas.read_csv(core_file_fp, dtype="string")
    # rename the TUBE_CODE_KEY column to TUBE_ID_KEY
    samples_df.rename(columns={TUBE_CODE_KEY: TUBE_ID_KEY}, inplace=True)

    # keep loading manifests in reverse chronological order until all samples
    # in the samples_df have been matched to a manifest
    merged_df, load_msgs_df = _lazy_load_manifests(samples_df, manifests_dir)
    if len(load_msgs_df) > 0:
        # write the load messages to a file
        timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        load_msgs_fp = os.path.join(
            os.path.dirname(core_file_fp),
            f"{timestamp_str}_manifest_load_messages.csv")
        load_msgs_df.to_csv(load_msgs_fp, index=False)
        raise ValueError(f"Errors occurred during manifest loading; "
                         f"see {load_msgs_fp}")

    extendable_metadata_df = _standardize_nph_input_metadata_df(
        merged_df, extraction_yyyy_mm)
    return extendable_metadata_df


def _lazy_load_manifests(samples_df, manifests_dir):
    aggregated_manifests_df = None
    merged_df = None
    missing_samples = samples_df[CORE_SAMPLE_NAME_KEY].tolist()
    manifests_found = False

    load_msgs_cols = ["load_message", MANIFEST_FILE_NAME_KEY,
                      MANIFEST_SAMPLE_ID_KEY]
    load_msgs_df = pandas.DataFrame(columns=load_msgs_cols)
    for curr_manifest_fp in _get_fps_youngest_to_oldest(manifests_dir):
        manifests_found = True

        # get the file name from the current manifest file path
        curr_manifest_fn = os.path.basename(curr_manifest_fp)

        if curr_manifest_fn.startswith("."):
            continue

        curr_manifest_df = load_df_with_best_fit_encoding(
            curr_manifest_fp, "\t", dtype="string")
        curr_manifest_df[MANIFEST_FILE_NAME_KEY] = curr_manifest_fn

        # drop curr_manifest_df records that have nan MANIFEST_SAMPLE_ID_KEY
        curr_manifest_df.dropna(subset=[MANIFEST_SAMPLE_ID_KEY], inplace=True)
        if curr_manifest_df.empty:
            err_msg = (f"Manifest has no records with a non-null "
                       f"'{MANIFEST_SAMPLE_ID_KEY}'")
            load_msgs_df = _append_load_msg_to_df(
                err_msg, curr_manifest_fn, "", load_msgs_df)
            continue

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
            merged_df.loc[missing_samples_mask, CORE_SAMPLE_ID_KEY].tolist()
        if len(missing_samples) == 0:
            break
    # end for each manifest

    if not manifests_found:
        load_msgs_df = _append_load_msg_to_df(
            f"No manifests found in the directory '{manifests_dir}'", "", "",
            load_msgs_df)
    else:
        # remove the MERGE_KEY column from the merged_df before returning
        merged_df.drop(columns=[MERGE_KEY], inplace=True)

        if len(missing_samples) > 0:
            sorted_missing_samples = sorted(missing_samples)
            for curr_missing_sample in sorted_missing_samples:
                load_msgs_df = _append_load_msg_to_df(
                    "Sample not found in any manifest", "", curr_missing_sample,
                    load_msgs_df)

        load_msgs_df = _sanity_check_aggregated_manifests(
            aggregated_manifests_df, load_msgs_df)
    # end if manifests weren't/were found

    if len(load_msgs_df) > 0:
        merged_df = None

    return merged_df, load_msgs_df


def _append_load_msg_to_df(load_msg, manifest_file, sample_id, load_msgs_df):
    load_msg_dict = {"load_message": load_msg,
                     MANIFEST_FILE_NAME_KEY: manifest_file,
                     MANIFEST_SAMPLE_ID_KEY: sample_id}
    add_df = pandas.DataFrame([load_msg_dict])
    output_df = pandas.concat([load_msgs_df, add_df], ignore_index=True)
    return output_df


def _append_load_msgs_to_df(load_msg, input_df, load_msgs_df):
    err_msg_df = input_df.loc[:,
                 [MANIFEST_FILE_NAME_KEY, MANIFEST_SAMPLE_ID_KEY]]
    err_msg_df["load_message"] = load_msg
    err_msg_df = err_msg_df[load_msgs_df.columns]
    load_msgs_df = pandas.concat([load_msgs_df, err_msg_df])
    return load_msgs_df


def _sanity_check_aggregated_manifests(aggregated_manifests_df, load_msgs_df):
    sorted_agg_manifests_df = aggregated_manifests_df.sort_values(
        by=[MANIFEST_SAMPLE_ID_KEY, MANIFEST_FILE_NAME_KEY])

    # the manifest column is definitely different between records from
    # different manifests, so pop it out of the aggregated_manifests_df
    # so we can check for duplicates across manifests
    sorted_agg_manifests_wo_manifest_col_df = \
        sorted_agg_manifests_df[
            sorted_agg_manifests_df.columns.drop(
                MANIFEST_FILE_NAME_KEY)].copy()

    # all rows that share sample ids with other row(s);
    # keep = false means all duplicates are id'd, including first one
    duplicate_sample_ids_mask = \
        sorted_agg_manifests_wo_manifest_col_df.duplicated(
            MANIFEST_SAMPLE_ID_KEY, keep=False)
    # all rows that are 100% the same as another row
    duplicates_mask = \
        sorted_agg_manifests_wo_manifest_col_df.duplicated(keep=False)

    if duplicates_mask.any():
        # order the df by the sample id and then by the manifest file name;
        # make sure to use df *with* manifest col so we can see which
        # manifest each duplicate came from
        duplicates_df = sorted_agg_manifests_df.loc[duplicates_mask]
        load_msgs_df = _append_load_msgs_to_df(
            "Duplicated record", duplicates_df, load_msgs_df)

    # combine duplicate_sample_ids_mask and duplicates_mask to get any
    # records that are not duplicates but share a sample id
    duplicate_sample_ids_only_mask = duplicate_sample_ids_mask & \
                                     ~duplicates_mask
    if duplicate_sample_ids_only_mask.any():
        duplicate_sample_ids_df = \
            sorted_agg_manifests_df.loc[duplicate_sample_ids_only_mask]
        load_msgs_df = _append_load_msgs_to_df(
            "Non-duplicate record with same sample id",
            duplicate_sample_ids_df, load_msgs_df)

    return load_msgs_df


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
    input_metadata_df[MATRIX_ID_KEY] = input_metadata_df[TUBE_ID_KEY]
    is_blank_mask = \
        input_metadata_df[CORE_SAMPLE_NAME_KEY].str.contains(BLANK_SUBSTRING)

    input_metadata_df.loc[is_blank_mask, COLLECTION_DATE_TIME_KEY] = \
        extraction_yyyy_mm

    input_metadata_df.loc[is_blank_mask, ADDITIVE_TREATMENT_KEY] = \
        DNA_RNA_SHIELD_VALUE

    input_metadata_df.loc[is_blank_mask, HOSTTYPE_SHORTHAND_KEY] = \
        "sterile_water_blank"
    input_metadata_df.loc[~is_blank_mask, HOSTTYPE_SHORTHAND_KEY] = "human"
    input_metadata_df.loc[~is_blank_mask, SAMPLE_TYPE_KEY] = \
        input_metadata_df.loc[~is_blank_mask, SAMPLE_TYPE_KEY].str.lower()

    input_metadata_df.loc[is_blank_mask, SAMPLETYPE_SHORTHAND_KEY] = \
        "control shield"
    input_metadata_df.loc[~is_blank_mask, SAMPLETYPE_SHORTHAND_KEY] = \
        input_metadata_df[SAMPLE_TYPE_KEY].str.lower()

    input_metadata_df.loc[is_blank_mask, HOST_SUBJECT_ID_KEY] = \
        input_metadata_df[CORE_SAMPLE_ID_KEY]
    return input_metadata_df


if __name__ == "__main__":
    # TODO: remove hardcoded arguments
    # raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_raw_metadata_short.xlsx"
    # raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_nph_metadata_short.xlsx"
    core_file_fp = "/Users/abirmingham/Desktop/metadata/nph/NPH_020 Sample Processing spreadsheet_SAS KL.csv"
    # core_file_fp = "/Users/abirmingham/Desktop/metadata/nph/NPH_017 Sample Processing spreadsheet_SAS KL.csv"
    # manifests_dir = "/Users/abirmingham/Desktop/metadata/nph/Manifests_06202024"
    manifests_dir = "/Users/abirmingham/Desktop/metadata/nph/manifest_08052024"
    extraction_yyyy_mm = "2024-08"
    output_dir = "/Users/abirmingham/Desktop/"
    output_base = "NPH_020"

    nph_config_dict = extract_config_dict(None, starting_fp=__file__)
    nph_extendable_metadata_df = make_nph_extendable_metadata_df(
        core_file_fp, manifests_dir, extraction_yyyy_mm)

    nph_transformers_dict = \
        {"format_real_vs_blanks_dates": format_real_vs_blanks_dates}

    write_extended_metadata_from_df(
        nph_extendable_metadata_df, nph_config_dict, output_dir, output_base,
        study_specific_transformers_dict=nph_transformers_dict)
