from typing import Union

from fastapi import FastAPI

import pandas as pd
import numpy as np
import string
import ast
import random

# ----------------------- Importing files from cloud --------------------------------------------------
# aust_user_item_df
url = 'https://drive.google.com/file/d/1SzdSHfM4HmWx1GQL49BsqfUIbpqZ-m8m/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
aust_user_item_df = pd.read_csv(url, sep = "|", encoding = "utf-8")


# filepath = "./Datasets/"
# aust_user_item_df = pd.read_csv(filepath + "aust_user_item_df.csv", sep = "|", encoding = "utf-8")
genres_list = list(aust_user_item_df.columns)[9:]


app = FastAPI()

# ---------------- Endpoint Nº1 ---------------------------------------------------------------------
@app.get("/PlayTimeGenre/{genero}")
def PlayTimeGenre(genero: str):
    # -*- coding: utf-8 -*-
    """
    Return the year with most hours played for a given genre
    """

    if genero not in genres_list:
        return {"El genero ingresado no se corresponde a uno activo en steam"}
    
    # building a temporary dataframe where store the relevant columns from aust_user_item_df for this endpoint --> hours played, year, choosen genre index ("1" or "0" if a row correspond to a game that can be related to this particular genre)
    columns_to_keep = ['items_playtime_forever', 'release_year']
    columns_to_keep.append(genero)
    temp_df = aust_user_item_df.drop(columns = list(set(aust_user_item_df.columns) - set(columns_to_keep)))
    
    # adding a new column that compute hours played for choosen genre
    temp_df['hours_played'] = temp_df['items_playtime_forever'] * temp_df[genero]
    
    # keeping only this new column of hours played for choosen game genre and release_year
    temp_df.drop(columns = ['items_playtime_forever', genero], inplace = True)

    # summarizing by year and store it in a dictionary
    hours_played_by_year = dict(temp_df.groupby('release_year')['hours_played'].sum())

    # where year with maximum hours played can be easily extracted
    year_max_hours = int(max(hours_played_by_year, key=hours_played_by_year.get))

    return {"El año con mas horas de juego acumuladas para el genero " + genero + " es:": year_max_hours}

