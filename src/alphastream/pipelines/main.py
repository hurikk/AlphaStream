from src.alphastream.pipelines import landing_layer, bronze_layer

def run():
    """Creates or updates the main_bronze table 
    in the bronze schema of the stock_database with 
    records of all stocks listed on B3"""
    landing_layer.insert_into_landing_layer(db_name="stock_database", schema_name="landing", table_name="main_landing")
    bronze_layer.insert_into_bronze_layer(db_name="stock_database", schema_name="bronze", 
                                                table_name="main_bronze")
    

if __name__ == "__main__":
    run()
    