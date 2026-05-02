# Superset Dashboard

This folder contains dashboard metadata and a visual preview for the local demo.

Recommended charts:

- Revenue trend from `revenue_daily`
- Orders by channel from `orders_gold`
- Top products from `top_products`
- Low inventory alerts from `inventory_risk`

Run Superset locally:

```bash
docker compose --profile dashboard up superset
```

Then open `http://localhost:8088` and sign in with `admin` / `admin`.

