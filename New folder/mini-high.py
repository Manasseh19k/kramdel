import pandas as pd
import re

# 1) Load your input Excel file
df = pd.read_excel("/mnt/data/Book1.xlsx")

records = []
# Regex to capture rate section, sub-section and field name
pattern = re.compile(r"negotiated_rates__(\d+)__(provider_groups|negotiated_prices)__(\d+)__(.+)")

for _, row in df.iterrows():
    base = {
        "name": row["name"],
        "billing_code_type": row["billing_code_type"],
        "billing_code_type_version": row["billing_code_type_version"],
        "billing_code": row["billing_code"],
    }
    # Build nested structure
    rates = {}
    for col, val in row.items():
        match = pattern.match(col)
        if not match or pd.isna(val):
            continue
        rate_idx, section, sub_idx, field = match.groups()
        rate_idx, sub_idx = int(rate_idx), int(sub_idx)
        rates.setdefault(rate_idx, {"provider_groups": {}, "negotiated_prices": {}})
        rates[rate_idx][section].setdefault(sub_idx, {})[field] = val

    # Flatten nested dict into records
    for rate in rates.values():
        for pg in rate["provider_groups"].values():
            for pr in rate["negotiated_prices"].values():
                rec = base.copy()
                rec.update({
                    "npi": pg.get("npi"),
                    "tin_type": pg.get("tin__type"),
                    "tin_value": pg.get("tin__value"),
                    "negotiated_type": pr.get("negotiated_type"),
                    "negotiated_rate": pr.get("negotiated_rate"),
                    "expiration_date": pr.get("expiration_date"),
                    "service_code": pr.get("service_code"),
                    "billing_class": pr.get("billing_class"),
                    "billing_code_modifier": pr.get("billing_code_modifier"),
                })
                records.append(rec)

# 3) Create a long‐form DataFrame and write to Excel
df_long = pd.DataFrame(records)
df_long.to_excel("/mnt/data/organize_data.xlsx", index=False)

# Preview first few rows
df_long.head()
