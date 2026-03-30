import pandas as pd

def transform_users(users_df: pd.DataFrame) -> pd.DataFrame:
    df = users_df.copy()

    df["email_domain"] = df["email"].apply(
        lambda x: x.split("@")[1] if isinstance(x, str) and "@" in x else None
    )
    df = df.drop(columns=["email"])

    return df