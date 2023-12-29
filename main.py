from typing import Union

from fastapi import FastAPI

import pandas as pd
import numpy as np
import string
import ast
import random

# ----------------------- Importing files from cloud --------------------------------------------------
# aust_user_item_df
url = 'https://drive.google.com/file/d/13UkQTS4yjnn0cVU_hQO-EMwbu81HaHpr/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
aust_user_item_df = pd.read_csv(url, sep = "|", encoding = "utf-8")

# reviews_posts
url = 'https://drive.google.com/file/d/1sf7msyTUs5QZJVTgO33jgwS5DlzvKkvw/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
reviews_posts = pd.read_csv(url, sep = "|", encoding = "utf-8")

# game_info
url = 'https://drive.google.com/file/d/1yT75secB_wVFY26bcka99lkmjlZwEpnN/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
game_info = pd.read_csv(url, sep = "|", encoding = "utf-8")

# games_cluser.csv
url = 'https://drive.google.com/file/d/1GdbzTGx7tI2_bi8-6U6ZN37oqfYY7reM/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
games_cluster = pd.read_csv(url, sep = "|", encoding = "utf-8")

# user_genre_mat_norm.csv
url = 'https://drive.google.com/file/d/11NX6OyT9Wjw4SXnAygc_mHLutpUMMkxi/view?usp=sharing'
url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
user_genre_mat_norm = pd.read_csv(url, sep = "|", encoding = "utf-8")

# user_item_genre.csv
url = 'https://drive.google.com/file/d/1WvwG4eRsIQBO9T8Q0eRd-3YOiTvKzZkC/view?usp=sharing'
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


# ---------------- Endpoint Nº4 ---------------------------------------------------------------------
@app.get("/UserWorstDeveloper/{anio}")
def UserWorstDeveloper(anio: int):
    # -*- coding: utf-8 -*-
    """
    Return the top 3 developers with games less recommended by users for a given year
    """

    # building a temporary dataframe where store the relevant columns from reviews_posts for this endpoint
    temp_df = reviews_posts[['reviews_item_id', 'reviews_recommend', 'reviews_year']]
    temp_df.columns = ['id', 'reviews_recommend', 'reviews_year']
    year_list = temp_df['reviews_year'].unique()

    if anio not in year_list:
        return {'El año ingresado no se encuentra en el rango a elegir'}

    temp_df['id'] = pd.to_numeric(temp_df['id'])

    # left join with game_info file in order to get 'developer' for each game
    temp_df = temp_df.merge(game_info, on='id', how = 'left')
    temp_df = temp_df[['developer', 'reviews_year', 'reviews_recommend']]

    # filtering by year
    condition = temp_df['reviews_year'] == anio
    develop_notrecom_lst = temp_df.loc[condition, :].groupby('developer')['reviews_recommend'].sum().sort_values().head(3)

    develop_user_notrecom_top_3 = ['None', 'None', 'None']

    # develop_notrecom_lst = temp_df.loc[condition, :].groupby('developer')['reviews_recommend'].sum().sort_values().head(3)

    develop_notrecom_lst = list(develop_notrecom_lst.index)

    return {'El top 3 de desarrolladores con menos recomendaciones en el año ' + str(anio) + ' es: ' + develop_notrecom_lst[0] + ', ' + develop_notrecom_lst[1] + ', ' + develop_notrecom_lst[2]}


# ---------------- Endpoint Nº5 ---------------------------------------------------------------------
@app.get("/Sentiment_Analysis/{desarrollador}")
def Sentiment_Analysis(desarrollador: str):
    # -*- coding: utf-8 -*-
    """
    Given a developer name, return a dictionary with the name of the developer as a key and a list
    with the total user's reviews categorized as negative, neutral and posivite as value
    """

    # building a temporary dataframe where store the relevant columns from reviews_posts for this endpoint
    temp_df = reviews_posts[['reviews_item_id', 'review_sentiment']]
    temp_df.columns = ['id', 'review_sentiment']

    temp_df['id'] = pd.to_numeric(temp_df['id'])

    # adding to each game the relevant info from game_info
    temp_df = temp_df.merge(game_info, on='id', how = 'left')
    temp_df = temp_df[['developer', 'review_sentiment']]

    # checking that input from user exists in database
    if str(desarrollador) not in temp_df['developer'].unique():
        return {'El desarrollador ingresado no tiene reseñas en steam'}

    # filtering for developer
    condition = temp_df['developer'] == desarrollador
    sentiment_lst = list(temp_df.loc[condition, 'review_sentiment'].values)

    # counting sentiment records for developer
    sent_neg = sentiment_lst.count(-1)
    sent_neu = sentiment_lst.count(0)
    sent_pos = sentiment_lst.count(1)

    # exporting result
    temp_list = ['negative = ' + str(sent_neg), 'neutral = ' + str(sent_neu), 'positive = ' + str(sent_pos)]
    result_dic = {str(desarrollador): temp_list}
    return {str(result_dic)}

# ---------------- Endpoint Nº 6 ---------------------------------------------------------------------
@app.get("/recomendacion_juego/{game_id}")
def recomendacion_juego(game_id: int):
    # -*- coding: utf-8 -*-
    """
    Recommend a list of 5 games simmilar to a given product id
    """
    
    if game_id not in games_cluster['id'].unique():
        return {'El id ingresado no se corresponde con un juego activo en steam'}

    # lets see at which cluster game_id belongs
    condition = games_cluster['id'] == game_id
    game_cluster_to_recommend = games_cluster[condition]['cluster']

    # filter to keep only games that belongs to that cluster
    condition = games_cluster['cluster'] == int(game_cluster_to_recommend)
    games_in_cluster = games_cluster.loc[condition,:]
    # and order them by total hours played, then keep the top 10
    games_rec_top10 = games_in_cluster.sort_values('total_hours', ascending = False).head(10)

    # compute 'probs' column by the proportion of hours played by a game in compared to the total in the top 10 list
    temp_total_hours = games_rec_top10['total_hours'].sum()
    games_rec_top10['prob'] = games_rec_top10['total_hours'] / temp_total_hours
    games_rec_top10 = games_rec_top10[['title', 'cluster', 'prob']]

    # recommending a list of games choosing randomly from games_rec_top10 but with a weight coming from 'probs'
    game_recom_lst = []
    recommended_game = np.random.choice(list(games_rec_top10['title']), 5, p = games_rec_top10['prob'], replace = False)
    game_recom_lst.insert(0, recommended_game)
    
    return {'Dado el juego ' + str(game_id) + ', recomendamos los siguientes: ' + str(list(game_recom_lst)[0])}

# ---------------- Endpoint Nº 7 ---------------------------------------------------------------------
@app.get("/recomendacion_usuario/{id_usuario}")
def recomendacion_usuario(id_usuario: str):
    # -*- coding: utf-8 -*-
    """
    Given an user id, recommend a list of 5 games for this particular user
    """

    if id_usuario not in user_genre_mat_norm['user_id'].unique():
        return {'El usuario ingresado no se corresponde con uno activo en steam'}

    # get list of genres (features)
    features = list(user_genre_mat_norm.columns)[2:]
    # y(x) --> user_id as a normalized vector of genre's components
    x = user_genre_mat_norm.loc[:, features].values
    y = user_genre_mat_norm.loc[:, ['user_id']].values

    # getting the input id
    user_id_to_recommend = id_usuario
    # looking for the row index of this particular user inside 'user_genre_mat_norm'
    user_index = user_genre_mat_norm.index[user_genre_mat_norm['user_id'] == user_id_to_recommend].tolist()
    # and using this row index to extract the user's normalized vector components (user(genre) coefficients)
    user_to_rec_features = x[user_index]

    # calculate the distances of every user with the one entered.
    # choosen euclidean instead of cosine distances because later one works better when components derived from text analysis are being compared and here are purely numeric
    distance_to_user = np.linalg.norm(x - user_to_rec_features)
    # and make a dataframe with users distances to the current one
    final_df = pd.DataFrame(y, columns = ['user_id'])
    final_df['distance_to_curr_user'] = distance_to_user

    # obtaining a list of the top 5 closest users to the current one
    user_simm_top5 = final_df.sort_values('distance_to_curr_user').head(6)
    user_simm_top5 = user_simm_top5.iloc[1:, :]
    # and choosing the closest one of all
    most_simm_user = user_simm_top5.iloc[0,0]

    # lets get which games the current user owns
    condition = user_item_genre['user_id'] == user_id_to_recommend
    user_to_recommend = user_item_genre.loc[condition, :]       # dataframe with the info of the current user
    user_owned_games = list(user_to_recommend['items_item_id']) # extract the list of current user owned games

    # and now which games the closest user to the current one has
    condition = user_item_genre['user_id'] == most_simm_user 
    simm_user = user_item_genre.loc[condition, :]               # dataframe with the info of the closest user to the current one (i.e. the one that the recommendation is going to be extracted from)
    # and keeping only the columns of interest sorted by most played games
    games_simm_user = simm_user[['items_item_id', 'items_item_name', 'items_playtime_forever']]
    # to get the list of games owned by the simmilar user, ordered by most played games first
    simm_user_games_list = list(games_simm_user.sort_values('items_playtime_forever', ascending = False)['items_item_id'])

    # and finally, loop over the simmilar user game list going from the most played to the lest, looking for the first game that isn't already owned by current user
    game_recommended = -1                                       # a -1 value acts as a flag for 'couldn't find a game in simmilar user that isn't already owned by current one'
    game_recom_lst = []
    for i in range(len(simm_user_games_list)):
        if simm_user_games_list[i] not in user_owned_games:
            game_recommended = simm_user_games_list[i]
            game_recom_lst.insert(0, game_recommended)
            if len(game_recom_lst) == 5:
                break
    
    if game_recommended == -1:
        return {'No se encontro un juego para recomendar al usuario ' + user_id_to_recommend}

    return {'Juego recomendado para el usuario ' + user_id_to_recommend + ': ' + str(game_recom_lst)}
