if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
from pycaret.regression import *
import pendulum
import wandb
import pandas as pd

def train_pycaret_model(data, train_data_proportion, target, fold, metric, save_model_path):
    """
    Train a PyCaret model using the provided data and parameters.

    Args:
        train_data (pd.DataFrame): The training data.
        test_data (pd.DataFrame): The test data.
        target (str): The name of the target column.
        fold (int): The number of folds for cross-validation.
        metric (str): The evaluation metric.
        save_model_path (str): The file path to save the trained model.

    Returns:
    """

    # Split data into train and test sets
    train_size = int(len(data) * train_data_proportion)
    train, test = data[0:train_size], data[train_size:len(data)]

    # Initialize the setup
    s1 = setup(data=train,
               test_data=test,
               target=target,
               fold=fold,
               session_id=123
               )

    best = compare_models(sort=metric)

    evaluate_model(best)

    metrics = pull()

    best_metric = metrics.iloc[0]
    print(best_metric)
    best_rmse = best_metric.loc['RMSE']
    best_mae = best_metric.loc['MAE']
    best_mse = best_metric.loc['MSE']
    model_name = best_metric.loc['Model']
    # Save the best model
    save_model(best, save_model_path)

    return best_rmse, best_mae, best_mse, model_name


@custom
def train(data, *args, **kwargs):
    # timestamp to datetime
    data['datetime'] = pd.to_datetime(data['timestamp'], unit='s')
    # drop timestamp
    data = data.drop(columns=['timestamp'])
    data['datetime'] = data['datetime'].dt.tz_localize(None)
    data.index = data['datetime']
    train_proportion = 0.8
    target = 'drybulb_temperature'
    fold = 4
    metric = 'RMSE'
    save_model_path = 'pame_ts_model'
    # TODO: Change user to your own name
    user = 'john'
    # Get current datetime
    now = pendulum.now().to_datetime_string()
    wandb.login(key="2b5d89854e8e5ce0c67bd34639c11aadc85ec14e")
    # Initiate a W&B experiment
    run = wandb.init(project='alto-academy-part7-timeseries', name=f'{user}_{now}')
    try:
        # Train model
        best_rmse, best_mae, best_mse, model_name = train_pycaret_model(data, train_proportion, target, fold, metric, save_model_path)

        # Log the model type
        wandb.config.update({"model_type": model_name})
        # Log metrics to W&B
        wandb.log({"RMSE": best_rmse, "MAE": best_mae, "MSE": best_mse})
        # Create a W&B artifact and add the model file to it
        artifact = wandb.Artifact('model', type='model')
        artifact.add_file(save_model_path + '.pkl')
        # Log the artifact
        run.log_artifact(artifact)
    except Exception as e:
        print(e)
    finally:
        # Close the W&B run
        run.finish()

    return {}
