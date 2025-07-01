import pandas as pd
import re

# 1) Load your input CSV
df = pd.read_csv("professional.csv")

records = []
# Regex to capture negotiated_prices fields under any negotiated_rates index
pattern = re.compile(r"negotiated_rates__(\d+)__negotiated_prices__(\d+)__(.+)")

# Define the base columns to carry forward
base_cols = ["billing_code", "billing_code_type", "billing_code_type_version", "name"]

for _, row in df.iterrows():
    base = {col: row[col] for col in base_cols}
    nested = {}
    # Collect all negotiated_prices fields for each (rate_idx, price_idx)
    for col, val in row.items():
        match = pattern.match(col)
        if match and pd.notna(val):
            rate_idx, price_idx, field = match.groups()
            key = (int(rate_idx), int(price_idx))
            nested.setdefault(key, {})[field] = val

    # Flatten into one record per negotiated_prices group
    for (rate_idx, price_idx), fields in nested.items():
        rec = base.copy()
        # Optionally include the indices if you need them
        # rec["rate_index"], rec["price_index"] = rate_idx, price_idx
        rec.update({
            "billing_class": fields.get("billing_class"),
            "billing_code_modifier": fields.get("billing_code_modifier"),
            "expiration_date": fields.get("expiration_date"),
            "negotiated_rate": fields.get("negotiated_rate"),
            "negotiated_type": fields.get("negotiated_type"),
            "service_code": fields.get("service_code"),


                # ► NEW FIELDS YOU WANT:
            "new_field_1":             fields.get("new_field_1"),
            "another_field":           fields.get("another_field"),
            # …and so on…

            
        })
        records.append(rec)

# 2) Create a long-form DataFrame and write to CSV
df_long = pd.DataFrame(records)
df_long.to_csv("organize_data.csv", index=False)

# 3) Preview the result
df_long.head()
