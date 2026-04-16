# Velora Analytics — Data Quality Findings

| Issue | Table | Count | How Handled |
|---|---|---|---|
| Missing product category | silver_products | 623 | Filled as "unknown" |
| Unapproved orders | silver_orders | 160 | Kept, flagged is_approved=0 |
| Undelivered orders | silver_orders | 2965 | Kept, flagged is_delivered=0 |
| Undefined payment type | silver_payments | 3 | Replaced with "unknown" |
| Negative delivery delay | silver_orders | Many | Expected — early deliveries |