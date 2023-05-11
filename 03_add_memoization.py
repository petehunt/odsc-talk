from helpers import memoize_to_s3
import duckdb
import pandas as pd


@memoize_to_s3("s3://mybucket/raw_customers.csv")
def raw_customers():
    return pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
    )


@memoize_to_s3("s3://mybucket/raw_orders.csv")
def raw_orders():
    return pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    )


@memoize_to_s3("s3://mybucket/raw_payments.csv")
def raw_payments():
    return pd.read_csv(
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"
    )


@memoize_to_s3("s3://mybucket/stg_customers.csv")
def stg_customers():
    source = raw_customers()
    return duckdb.query(
        "select id as customer_id, first_name, last_name from source"
    ).to_df()


@memoize_to_s3("s3://mybucket/stg_orders.csv")
def stg_orders():
    source = raw_orders()
    return duckdb.query(
        "select id as order_id, user_id as customer_id, order_date, status from source"
    ).to_df()


@memoize_to_s3("s3://mybucket/stg_payments.csv")
def stg_payments():
    source = raw_payments()
    return duckdb.query(
        "select id as payment_id, order_id, payment_method, amount / 100 as amount from source"
    ).to_df()


@memoize_to_s3("s3://mybucket/customers.csv")
def customers():
    customers = stg_customers()
    orders = stg_orders()
    payments = stg_payments()
    return duckdb.query(
        """
with customer_orders as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),

customer_payments as (
    select
        orders.customer_id,
        sum(amount) as total_amount
    from payments
    left join orders on
         payments.order_id = orders.order_id
    group by orders.customer_id
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value
    from customers
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id
    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id
)

select * from final"""
    ).to_df()


@memoize_to_s3("s3://mybucket/orders.csv")
def orders():
    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]
    orders = stg_orders()
    payments = stg_payments()
    amounts = [
        f"sum(case when payment_method = '{payment_method}' then amount else 0 end) as {payment_method}_amount"
        for payment_method in payment_methods
    ]
    final_amounts = [
        f"order_payments.{payment_method}_amount" for payment_method in payment_methods
    ]
    return duckdb.query(
        f"""
with order_payments as (
    select
        order_id,
        {",".join(amounts)},
        sum(amount) as total_amount
    from payments
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        {",".join(final_amounts)},
        order_payments.total_amount as amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
)

select * from final
    """
    ).to_df()


print(customers())
print(orders())
from helpers import s3_read_csv

print(s3_read_csv("s3://mybucket/customers.csv"))
print(s3_read_csv("s3://mybucket/orders.csv"))
