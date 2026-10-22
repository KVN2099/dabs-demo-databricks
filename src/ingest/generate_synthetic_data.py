# Databricks notebook source
# MAGIC %md
# MAGIC # Land synthetic customers and transactions
# MAGIC
# MAGIC Lower environments never receive production PII. Every environment is fed
# MAGIC synthetic data with the same shape, so masks, filters and policies are
# MAGIC exercised on realistic columns. Volume scales per target (`customer_count`).

# COMMAND ----------

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
customer_count = int(dbutils.widgets.get("customer_count"))

bronze = f"`{catalog}`.`{bronze_schema}`"
silver = f"`{catalog}`.`{silver_schema}`"

regions = F.array(*[F.lit(r) for r in ["NA", "EMEA", "LATAM", "APAC"]])
first_names = F.array(*[F.lit(n) for n in ["Ana", "Ben", "Chen", "Diego", "Emma", "Farah", "Gus", "Hana", "Ivan", "Jade"]])
last_names = F.array(*[F.lit(n) for n in ["Lopez", "Smith", "Wang", "Rossi", "Khan", "Silva", "Novak", "Okafor", "Kim", "Moreau"]])
merchants = F.array(*[F.lit(m) for m in ["Grocer", "Airline", "Cafe", "Fuel", "Online", "Pharmacy"]])
categories = F.array(*[F.lit(c) for c in ["groceries", "travel", "dining", "fuel", "retail", "health"]])


def pick(options, seed_col):
    return F.element_at(options, (F.abs(F.hash(seed_col)) % F.size(options) + 1).cast("int"))


def digits(seed_col, n):
    return F.lpad((F.abs(F.xxhash64(seed_col)) % F.lit(10**n)).cast("string"), n, "0")

# COMMAND ----------

customers_raw = (
    spark.range(1, customer_count + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("first", pick(first_names, F.concat(F.col("customer_id"), F.lit("f"))))
    .withColumn("last", pick(last_names, F.concat(F.col("customer_id"), F.lit("l"))))
    .select(
        "customer_id",
        F.concat_ws(" ", "first", "last").alias("full_name"),
        F.concat(F.lower("first"), F.lit("."), F.lower("last"), F.col("customer_id"), F.lit("@Example.com")).alias("email"),
        F.concat(F.lit("+1 (555) "), digits(F.concat("customer_id", F.lit("p")), 3), F.lit("-"), digits(F.concat("customer_id", F.lit("q")), 4)).alias("phone"),
        F.concat_ws("-", digits(F.concat("customer_id", F.lit("a")), 3), digits(F.concat("customer_id", F.lit("b")), 2), digits(F.concat("customer_id", F.lit("c")), 4)).alias("ssn"),
        F.date_sub(F.lit("2004-01-01").cast("date"), (F.abs(F.hash("customer_id")) % 18000).cast("int")).alias("date_of_birth"),
        pick(regions, F.col("customer_id")).alias("region"),
        (F.abs(F.hash(F.concat("customer_id", F.lit("s")))) % 551 + 300).cast("int").alias("credit_score"),
        F.current_timestamp().alias("created_at"),
    )
)
customers_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{bronze_schema}.customers_raw")

transactions_raw = (
    spark.range(1, customer_count * 10 + 1)
    .withColumnRenamed("id", "txn_id")
    .withColumn("customer_id", (F.abs(F.hash("txn_id")) % customer_count + 1).cast("bigint"))
    .select(
        "txn_id",
        "customer_id",
        F.concat(F.lit("4"), digits(F.col("customer_id"), 15)).alias("card_number"),
        pick(merchants, F.col("txn_id")).alias("merchant"),
        pick(categories, F.col("txn_id")).alias("category"),
        ((F.abs(F.hash(F.concat("txn_id", F.lit("amt")))) % 50000) / 100).cast("decimal(12,2)").alias("amount"),
        F.timestamp_seconds(F.unix_timestamp() - F.abs(F.hash("txn_id")) % (90 * 86400)).alias("txn_ts"),
    )
)
transactions_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{bronze_schema}.transactions_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze -> silver
# MAGIC `INSERT OVERWRITE` keeps the tags, masks and row filters that `apply_governance` attached.

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {silver}.customers
SELECT customer_id, full_name, lower(email), phone, ssn, date_of_birth, region, credit_score, created_at
FROM {bronze}.customers_raw
""")

spark.sql(f"""
INSERT OVERWRITE {silver}.transactions
SELECT t.txn_id, t.customer_id, t.card_number, t.merchant, t.category, t.amount, c.region, t.txn_ts
FROM {bronze}.transactions_raw t
JOIN {bronze}.customers_raw c USING (customer_id)
WHERE t.amount > 0
""")

for table in ("customers", "transactions"):
    print(table, spark.table(f"{catalog}.{silver_schema}.{table}").count())
