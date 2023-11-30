import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler



def load_data(path: str = None, drop_unnamed: bool = True):
    """
    This function takes a path to a CSV file and loads it to create a pandas DataFrame

    :param  path: str,  path to the csv

    :return df: pd.DataFrame
    """

    df = pd.read_csv(path)
    if drop_unnamed: df.drop(columns="Unnamed: 0", inplace = True, errors="ignore")
    return df

def create_X_y(data: pd.DataFrame = None, targer: str = None):
    """
    This function takes in a Pandas DataFrame and splits the columns
    into a target column and a set of predictor variables, i.e. X & y.
    These two splits of the data will be used to train a supervised 
    machine learning model.

    :param      data: pd.DataFrame, dataframe containing data for the 
                      model
    :param      target: str (optional), target variable that you want to predict

    :return     X: pd.DataFrame
                y: pd.Series
    """

    if target not in data.columns:
        raise Exception(f"Target: {target} is not a column of the DataFrame: {data}")
    
    X = data.drop(columns=[target])
    y = data[target]
    return X, y

def train_random_forest_regressor(
        X: pd.DataFrame = None, 
        y: pd.Series = None,
        split: float = 0.8,
        random_state: int = 0
):
    """
    This function takes the predictor and target variables and
    trains a Random Forest Regressor model across K folds. Using
    cross-validation, performance metrics will be output for each
    fold during training.

    :param      X: pd.DataFrame, predictor variables
    :param      y: pd.Series, target variable
    :param      split

    :return
    """
    # Loading the model and data 
    model = RandomForestRegressor()
    scaler = StandardScaler()

    # Train and test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=split, random_state=random_state)

    # Scale the data 
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    # Train the model 
    trained_model = model.fit(X_train, y_train)

    # Model Predictions
    y_pred = trained_model.predict(X_test)

    # Compute the Model Accuracy
    mae = mean_absolute_error(y_test, y_pred)
    print(mae)