# Importing required libraries
import numpy as np
import pandas as pd


# Defining dataset generation parameters
NUM_ROWS = 500000  # Generating big data scale dataset
NUM_FEATURES = 20


# Defining wine category list
WINE_TYPES = ["red", "white", "rose"]


# Generating synthetic dataset
def generating_big_wine_dataset():

    # Initialising storage list
    data_storage = []

    for _ in range(NUM_ROWS):

        # Selecting random wine category
        wine_category = np.random.choice(WINE_TYPES)

        # Generating random chemical values
        chemical_values = np.random.uniform(0, 10, NUM_FEATURES)

        # Appending record
        data_storage.append([wine_category] + chemical_values.tolist())

    # Creating dataframe
    columns = ["wine_type"] + [f"feature_{i}" for i in range(NUM_FEATURES)]
    df = pd.DataFrame(data_storage, columns=columns)

    # Saving dataset
    df.to_csv("data/generated_big_wine_dataset.csv", index=False)

    print("Dataset generating completed")


if __name__ == "__main__":
    generating_big_wine_dataset()
