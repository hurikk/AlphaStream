from src.alphastream.pipelines import bronze_layer

def run():
    """Creates or updates the main_bronze table 
    in the bronze schema of the stock_database with 
    records of all stocks listed on B3"""
    bronze_layer.insert_into_bronze_layer(db_name="stock_database", schema_name="bronze", 
                                                table_name="main_bronze")
    

if __name__ == "__main__":
    run()
    