import pandas as pd

# 1) load your long‐form CSV
df = pd.read_csv("organize_data.csv")

# 2) parse the existing MM/DD/YYYY dates and re-format them as YYYY-MM-DD
df["expiration_date"] = (
    pd.to_datetime(df["expiration_date"], format="%m/%d/%Y", errors="coerce")
      .dt.strftime("%Y-%m-%d")
)

# 3) write back out (or continue your pipeline)
df.to_csv("organize_data_fixed_dates.csv", index=False)
