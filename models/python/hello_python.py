def model(dbt, session):

    dbt.config(materialized="table")

    data = [("Snowpark", 1), ("dbt", 2), ("POC", 3)]
    df = session.create_dataframe(data, schema=["name", "id"])

    return df
