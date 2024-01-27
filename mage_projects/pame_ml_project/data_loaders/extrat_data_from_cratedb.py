if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
from utils.database import AltoCrateDB
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    table_name = kwargs['raw_table']
    crateDB = AltoCrateDB(host='infra_cratedb')

    data = crateDB.query_data(table_name=table_name, filters=None)
    df = pd.DataFrame(data)
    print("Data extracting done!")

    return df
