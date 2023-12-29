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

# reviews_posts
url = 'https://drive.google.com/file/d/1_iZTiQ5U4WywpJt9IsDSKDnq8gEFklyT/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
reviews_posts = pd.read_csv(url, sep = "|", encoding = "utf-8")

# game_info
url = 'https://drive.google.com/file/d/1TYQw_uxeZWOoON8JsgHVVIo-j8ED4bce/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
game_info = pd.read_csv(url, sep = "|", encoding = "utf-8")

# games_cluser.csv
url = 'https://drive.google.com/file/d/1JIVdDLEeXVaT6SoLoA1bQ-NMNl_Mk5SR/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
games_cluster = pd.read_csv(url, sep = "|", encoding = "utf-8")

# user_genre_mat_norm.csv
url = 'https://drive.google.com/file/d/1eANB9sJFw1cueW-zmoQvdb_Zsf5-l7eb/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
user_genre_mat_norm = pd.read_csv(url, sep = "|", encoding = "utf-8")

# user_item_genre.csv
url = 'https://drive.google.com/file/d/16awWTbiUHDAlnMEIucN5JjUqekO4NQN-/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
user_item_genre = pd.read_csv(url, sep = "|", encoding = "utf-8")

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

# ---------------- Endpoint Nº2 ---------------------------------------------------------------------
@app.get("/UserForGenre/{genero}")
def UserForGenre(genero: str):
    # -*- coding: utf-8 -*-
    """
    Return the user with more playtime hours in games with the given genre and a list
    of how many hours played by years
    """

    if genero not in genres_list:
        return {"El genero ingresado no se corresponde a uno activo en steam"}
    
    # building a temporary dataframe where store the relevant columns from aust_user_item_df for this endpoint --> user_id, hours played, year, choosen genre index ("1" or "0" if a row correspond to a game that can be related to this particular genre)
    columns_to_keep = ['user_id', 'items_playtime_forever', 'release_year']
    columns_to_keep.append(genero)
    temp_df = aust_user_item_df.drop(columns = list(set(aust_user_item_df.columns) - set(columns_to_keep)))

    # extracting the user_id that played the most for the choosen game genre
    temp_df['hours_played'] = temp_df['items_playtime_forever'] * temp_df[genero]
    temp_df.drop(columns = ['items_playtime_forever', genero], inplace = True)
    users_hours_played_dict = dict(temp_df.groupby('user_id')['hours_played'].sum())
    user_max_hours = max(users_hours_played_dict, key = users_hours_played_dict.get)

    # summarizing by year the user_id that played the most
    condition = temp_df['user_id'] == user_max_hours
    user_max_hours_year = temp_df[condition].groupby('release_year')['hours_played'].sum()
    user_max_hours_year.sort_index()

    result_list = []
    for i in range(len(user_max_hours_year)):
        temp_dic = {'Año': int(user_max_hours_year.index[i]), 'Horas': user_max_hours_year.values[i]}
        result_list.insert(0, temp_dic)
    
    return_string = "Usuario con mas horas jugadas para Genero " + genero + ": " + user_max_hours + ", Horas jugadas: "
    return_string = return_string + str(user_max_hours_year)
    return {return_string}


# ---------------- Endpoint Nº3 ---------------------------------------------------------------------
@app.get("/UserRecommend/{anio}")
def UserRecommend(anio: int):
    # -*- coding: utf-8 -*-
    """
    Return the top 3 games with the best user's recommendation for a given year
    """

    # building a temporary dataframe where store the relevant columns from reviews_posrt for this endpoint
    temp_df = reviews_posts[['reviews_item_id', 'reviews_recommend', 'reviews_year']]
    year_list = temp_df['reviews_year'].unique()

    if anio not in year_list:
        return {'El año ingresado no se encuentra en el rango a elegir'}

    condition = temp_df['reviews_year'] == anio
    recom_game_year_lst = temp_df[condition].groupby('reviews_item_id')['reviews_recommend'].sum().sort_values(ascending=False).head(3)

    # game_info = pd.read_csv(filepath + "game_info.csv", delimiter= "|", encoding="utf-8")
    game_user_recom_top_3 = ['None', 'None', 'None']
    for i in range(3):
        temp_game_index = recom_game_year_lst.index[i]
        condition = game_info['id'] == int(temp_game_index)
        game_user_recom_top_3[i] = game_info.loc[condition, 'title'].values[0]
    
    return {"Top 3 de juegos mas recomendados en el año " + str(anio) + ": " + game_user_recom_top_3[0] + ', ' + game_user_recom_top_3[1] + ', ' + game_user_recom_top_3[2]}

