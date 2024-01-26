if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
from utils.database import AltoCrateDB

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    table_name = kwargs['raw_table']
    
    crateDB = AltoCrateDB()

    try:
        crateDB.insert_data(table_name=table_name, data=data)
        print("Successfully inserted data into CrateDB")
    except Exception as e:
        print(f"Cannot insert data to CrateDB due to the follow error {e}")
        raise Exception(f"Cannot insert data to CrateDB due to the follow error {e}")

