from src.alphastream.pipelines import landing_layer, bronze_layer, silver_layer

def run():
    """Creates or updates the main_bronze table 
    in the bronze schema of the stock_database with 
    records of all stocks listed on B3"""
    landing_layer.insert_into_landing_layer(db_name="stock_database", schema_name="landing", table_name="main_landing")
    bronze_layer.insert_into_bronze_layer(db_name="stock_database", schema_name="bronze", table_name="main_bronze")
    silver_layer.insert_into_silver_layer(db_name="stock_database", schema_name="silver", table_name="main_silver")

if __name__ == "__main__":
    run()
    