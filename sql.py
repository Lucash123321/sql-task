from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductsData").getOrCreate()

products_data = []

categories_data = []

categories_of_products_data = []


products = spark.createDataFrame(products_data)
categories = spark.createDataFrame(categories_data)
categories_of_products = spark.createDataFrame(categories_of_products_data)


products.createOrReplaceTempView("products")
categories.createOrReplaceTempView("categories")
categories_of_products.createOrReplaceTempView("product_categories")

results = spark.sql("SELECT product_name, category_name FROM products \
                    LEFT JOIN product_categories ON products.product_id = product_categories.product_id \
                    LEFT JOIN categories c ON product_categories.category_id = categories.category_id")

results.show()
