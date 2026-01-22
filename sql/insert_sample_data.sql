INSERT INTO stores (store_code, store_name, city, state, region, store_type, opened_date)
VALUES
('STR001', 'FreshMart Downtown', 'Toronto', 'Ontario', 'East', 'Supermarket', '2018-05-10'),
('STR002', 'FreshMart Uptown', 'Toronto', 'Ontario', 'East', 'Supermarket', '2020-09-15'),
('STR003', 'QuickBuy Central', 'Vancouver', 'British Columbia', 'West', 'Convenience', '2019-03-22'),
('STR004', 'Wholesale Hub', 'Calgary', 'Alberta', 'West', 'Wholesale', '2017-11-01')
ON CONFLICT DO NOTHING;

INSERT INTO products
(sku_code, product_name, brand, category, sub_category, pack_size, unit_price, launch_date)
VALUES
('SKU1001', 'Sparkle Soda Lime', 'Sparkle', 'Beverages', 'Carbonated', '500ml', 1.99, '2021-01-01'),
('SKU1002', 'Sparkle Soda Orange', 'Sparkle', 'Beverages', 'Carbonated', '500ml', 1.99, '2021-01-01'),
('SKU2001', 'Crunchy Chips Classic', 'Crunchy', 'Snacks', 'Chips', '150g', 2.49, '2020-06-15'),
('SKU2002', 'Crunchy Chips BBQ', 'Crunchy', 'Snacks', 'Chips', '150g', 2.49, '2020-06-15'),
('SKU3001', 'Daily Milk 2%', 'DailyDairy', 'Dairy', 'Milk', '1L', 2.79, '2019-02-01')
ON CONFLICT DO NOTHING;

INSERT INTO calendar
(date_id, year, month, day, week_of_year, quarter, day_of_week, is_weekend)
VALUES
('2025-01-01', 2025, 1, 1, 1, 1, 3, FALSE),
('2025-01-02', 2025, 1, 2, 1, 1, 4, FALSE),
('2025-01-03', 2025, 1, 3, 1, 1, 5, FALSE),
('2025-01-04', 2025, 1, 4, 1, 1, 6, TRUE),
('2025-01-05', 2025, 1, 5, 1, 1, 7, TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO sales_fact
(date_id, store_id, product_id, units_sold, gross_sales, discount_amount, net_sales, promo_flag, inventory_on_hand)
VALUES
('2025-01-01', 1, 1, 120, 238.80, 20.00, 218.80, TRUE, 500),
('2025-01-01', 1, 3, 85, 211.65, 0.00, 211.65, FALSE, 300),
('2025-01-01', 2, 1, 95, 189.05, 15.00, 174.05, TRUE, 420),
('2025-01-02', 3, 2, 60, 119.40, 0.00, 119.40, FALSE, 200),
('2025-01-02', 4, 5, 150, 418.50, 25.00, 393.50, TRUE, 800)
ON CONFLICT DO NOTHING;
