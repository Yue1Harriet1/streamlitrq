import pandas as pd

def save_uploaded_file(file, path:str):
    return ""


def get_start_task_id(df: pd.DataFrame) -> int:
    """
    Generate an ID for a new task:
        -> check for the last used ID
        -> increment by 1

    Args:
        df: dataframe with process information and related task IDs.

    Returns:
        int: new task ID.
    """
    return df["task_id"].max() + 1 if df["task_id"].values else 1