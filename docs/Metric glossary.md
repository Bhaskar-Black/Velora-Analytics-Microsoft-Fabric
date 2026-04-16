# Velora Analytics — Metric Glossary

| Metric | Definition | Source Table |
|---|---|---|
| Total Revenue | Sum of all payment_value | fact_payments |
| Total Orders | Count of distinct order_id | fact_orders |
| Late Delivery Rate | Sum of is_late_delivery / Total delivered orders | fact_orders |
| Repeat Customer Rate | Customers with is_repeat_customer=1 / Total customers | dim_customer |
| Avg Order Value | Average of total_order_value | fact_orders |
| Avg Delay Days | Average of delivery_delay_days | fact_orders |
| Installment Rate | Sum of is_installment / Total payments | fact_payments |