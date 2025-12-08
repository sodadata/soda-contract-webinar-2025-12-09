-- Create orders table for Soda Contract Webinar Demo
-- This table matches the contract schema defined in contracts/webinardb/postgres/public/orders.yaml

CREATE TABLE IF NOT EXISTS public.orders (
    order_id text NOT NULL,
    customer_id text NOT NULL,
    order_date date NOT NULL,
    shipping_date date NOT NULL,
    shipping_address text NOT NULL,
    amount numeric(10, 2) NOT NULL,
    status text NOT NULL,
    country_code text NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (order_id)
) TABLESPACE pg_default;

-- Add index on customer_id for better query performance (optional)
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON public.orders(customer_id);

-- Add index on order_date for date range queries (optional)
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON public.orders(order_date);

