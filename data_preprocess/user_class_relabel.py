"""
Create label for each class based on tag_list and latest_cc.
"""

import glob
import numpy as np
import pandas as pd


def fast_label_mapping(label_list, mapping_dict):
    """
    Maps a list of labels to a corresponding mapping dictionary.

    Args:
        label_list (list): A list of labels to be mapped.
        mapping_dict (dict): A dictionary containing the mapping of labels.

    Returns:
        str: The mapped label based on the mapping dictionary.


    """
    try:
        labels = [
            mapping_dict.get(tag, "neutral")
            for tag in label_list
            if tag in mapping_dict
        ]
        if not labels:
            return "neutral"
        if "bad" in labels:
            return "bad"
        elif "good" in labels:
            return "good"
        else:
            return "neutral"
    except:
        return "neutral"


def main():
    """
    Main function to create label for each class based on tag_list and latest_cc.
    """

    paths = glob.glob("/opt/oneworks/behavior_catcher/class_*.csv")
    paths.sort()
    for path in paths:
        print(f"Processing {path}")
        df = pd.read_csv(path)

        # Create label from tag_list
        tag_label = pd.read_csv("Tag_Label.csv")
        mapping_dict = dict(zip(tag_label["tag"], tag_label["Label"].str.lower()))
        df["tag_list"] = df.tag_list.str.split(",")
        df["label_tag"] = df["tag_list"].apply(
            lambda x: fast_label_mapping(x, mapping_dict)
        )

        # Create label from latest_cc
        cc_label = pd.read_csv("CC_Label.csv")
        mapping_dict = dict(zip(cc_label["CC"], cc_label["Label"].str.lower()))
        df["label_cc"] = df.latest_cc.apply(lambda x: mapping_dict.get(x, mapping_dict))

        # Combine label_tag and label_cc
        condition_bad = (df["label_tag"] == "bad") | (df["label_cc"] == "bad")
        condition_good = (df["label_tag"] == "good") | (df["label_cc"] == "good")
        df["label"] = np.select([condition_bad, condition_good], [0, 2], default=1)
        df = df[["date", "cust_id", "label"]]
        df.to_csv(
            f"/opt/oneworks/behavior_catcher/label_{path.split('_')[-1]}", index=False
        )


if __name__ == "__main__":
    main()
