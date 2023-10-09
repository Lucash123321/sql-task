from pyspark.sql import SparkSession


class ProductData:
    def __init__(self, products_data, categories_data, categories_of_products_data):
        self.spark = SparkSession.builder.appName("ProductsData").getOrCreate()

        products = self.spark.createDataFrame(products_data)
        categories = self.spark.createDataFrame(categories_data)
        categories_of_products = self.spark.createDataFrame(categories_of_products_data)

        products.createOrReplaceTempView("products")
        categories.createOrReplaceTempView("categories")
        categories_of_products.createOrReplaceTempView("product_categories")

    def get_categories_of_products(self):
        results = self.spark.sql("SELECT product_name, category_name FROM products \
                        LEFT JOIN categories_of_products ON products.product_id = categories_of_products.product_id \
                        LEFT JOIN categories ON categories_of_products.category_id = categories.category_id")

        results.show()


if __name__ == '__main__':
    pass
