# Power BI Integration Guide (Databricks + Delta Lake)

## Connection Methods
1. Databricks Connector (DirectQuery) - Use Databricks SQL Endpoint for live queries.
2. Import Mode - Export CSV/Parquet from Gold and import into Power BI for faster dashboards with refresh schedule.

## Recommended SQL Queries (Databricks SQL)
-- Monthly Sales
SELECT year, month, SUM(total_sales) AS monthly_sales
FROM delta.`/mnt/adls/retail/gold/`
GROUP BY year, month
ORDER BY year, month;

-- Top 10 Products by Revenue
SELECT product_id, SUM(total_sales) AS revenue
FROM delta.`/mnt/adls/retail/gold/`
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

## Sample DAX Measures (Power BI)
-- Total Revenue
TotalRevenue = SUM('gold_table'[total_sales])

-- Total Units Sold
TotalUnits = SUM('gold_table'[total_quantity])

-- Average Order Value (AOV)
AOV = DIVIDE([TotalRevenue], [TotalUnits], 0)

-- YoY Growth (requires Date table)
YoYRevenue = 
VAR Curr = CALCULATE([TotalRevenue], SAMEPERIODLASTYEAR('Date'[Date]))
RETURN DIVIDE([TotalRevenue] - Curr, Curr, 0)

## Report Layout Suggestions
- KPI cards: TotalRevenue, TotalUnits, UniqueCustomers, AOV
- Time series line chart: Monthly Sales
- Bar chart: Top 10 Products by Revenue
- Map/Bar chart: Sales by Region
- Table: Recent Orders (link to transaction drill-through)

## Data Modeling Tips
- Create a Date dimension table and mark as Date Table in Power BI.
- Use DirectQuery for low-latency live dashboards; prefer Import mode for complex visuals.
- Reduce columns to necessary fields to improve performance in Power BI.
