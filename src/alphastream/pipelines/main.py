from src.alphastream.pipelines import landing_layer, bronze_layer, silver_layer, gold_layer


def run() -> None:
    """Runs the full pipeline to update B3 stock tables.

    Flows through the landing, bronze, silver, and gold layers,
    inserting or updating records for all stocks listed on B3
    into their corresponding tables in the stock_database.

    Returns:
        None

    Raises:
        Exception: If any of the layers fail during data insertion
            (the exact type depends on the implementation of each
            layer — landing_layer, bronze_layer, silver_layer, or
            gold_layer).
    """
    landing_layer.insert_into_landing_layer(
        db_name="stock_database", schema_name="landing", table_name="main_landing"
    )
    bronze_layer.insert_into_bronze_layer(
        db_name="stock_database", schema_name="bronze", table_name="main_bronze"
    )
    silver_layer.insert_into_silver_layer(
        db_name="stock_database", schema_name="silver", table_name="main_silver"
    )
    gold_layer.insert_into_gold_layer(
        db_name="stock_database", schema_name="gold", table_name="main_gold"
    )


if __name__ == "__main__":
    run()