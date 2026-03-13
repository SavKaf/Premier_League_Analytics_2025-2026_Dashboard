# Databricks notebook source
import requests
import json
import pandas as pd

# COMMAND ----------

# Set the connection
API_KEY = "API_KEY"
BASE_URL = "https://api.football-data.org/v4/"
headers = {"X-Auth-Token": API_KEY}

# Get the Premier League matches data only
response = requests.get(
    BASE_URL + "competitions/PL/matches",
    headers=headers
)
# Print the Dictionary of json file
data = response.json()
#print(data)

# COMMAND ----------

print(data.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC Create Matches Dataframe

# COMMAND ----------

df_matches = pd.json_normalize(data["matches"])
df_matches = df_matches.drop(columns=["referees","odds.msg","season.winner"])
print(df_matches.head())

# COMMAND ----------

print(df_matches.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Standings Table

# COMMAND ----------

# Standings Table
response_standings = requests.get(
    BASE_URL + "competitions/PL/standings",
    headers=headers
)

data_standings = response_standings.json()
print(data_standings.keys())

# COMMAND ----------

standings = data_standings["standings"][0]["table"]

df_standings = pd.json_normalize(standings)

# Remove team.area
df_standings = df_standings.drop(columns=["team.area"], errors="ignore")

print(df_standings.head())

# COMMAND ----------

print(df_standings.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Top Scorers Dataframe

# COMMAND ----------

# Top Scorers (default top 10)
response_scorers = requests.get(
    BASE_URL + "competitions/PL/scorers",
    headers=headers
)

data_scorers = response_scorers.json()

# COMMAND ----------

df_top_scorers = pd.json_normalize(data_scorers["scorers"])
# Remove columns
df_top_scorers = df_top_scorers.drop(columns=["player.shirtNumber","player.position"], errors="ignore")
print(df_top_scorers.head())

# COMMAND ----------

print(df_top_scorers.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Build Dimension and Fact tables

# COMMAND ----------

# # Dimension team (Home and Away) from df_matches
# df_matches: home & away teams
teams_home = df_matches[['homeTeam.id', 'homeTeam.name', 'homeTeam.shortName', 'homeTeam.tla', 'homeTeam.crest']].rename(
    columns={'homeTeam.id':'team_id','homeTeam.name':'team_name','homeTeam.shortName':'team_shortName',
             'homeTeam.tla':'team_tla','homeTeam.crest':'team_crest_url'}
)
teams_away = df_matches[['awayTeam.id', 'awayTeam.name', 'awayTeam.shortName', 'awayTeam.tla', 'awayTeam.crest']].rename(
    columns={'awayTeam.id':'team_id','awayTeam.name':'team_name','awayTeam.shortName':'team_shortName',
             'awayTeam.tla':'team_tla','awayTeam.crest':'team_crest_url'}
)

# Concat and remove Duplicates
dimension_team = pd.concat([teams_home, teams_away], axis=0).drop_duplicates(subset=['team_id']).reset_index(drop=True)

# Add columns from df_top_scorers
team_extra = df_top_scorers[['team.id','team.venue']].rename(columns={'team.id':'team_id','team.venue':'team_venue'}).drop_duplicates(subset=['team_id'])
dimension_team = dimension_team.merge(team_extra, on='team_id', how='left')

# COMMAND ----------

print(dimension_team.columns)

# COMMAND ----------

# Dimension Competition from df_matches
dimension_competition = df_matches[['competition.id','competition.name','competition.code','competition.type','competition.emblem']].rename(
    columns={'competition.id':'competition_id','competition.name':'competition_name',
             'competition.code':'competition_code','competition.type':'competition_type',
             'competition.emblem':'competition_emblem_url'}
).drop_duplicates(subset=['competition_id'])

# COMMAND ----------

print(dimension_competition.columns)

# COMMAND ----------

# Season Dimension
dimension_season = df_matches[['season.id','season.startDate','season.endDate','season.currentMatchday']].rename(
    columns={'season.id':'season_id','season.startDate':'start_date','season.endDate':'end_date',
             'season.currentMatchday':'current_matchday'}
).drop_duplicates(subset=['season_id'])

# COMMAND ----------

print(dimension_season.columns)

# COMMAND ----------

# Area Dimension
dimension_area = df_matches[['area.id','area.name','area.code','area.flag']].rename(
    columns={'area.id':'area_id','area.name':'area_name','area.code':'area_code','area.flag':'area_flag_url'}
).drop_duplicates(subset=['area_id'])

# COMMAND ----------

print(dimension_area.columns)

# COMMAND ----------

# Player Dimension
dimension_player = df_top_scorers[['player.id','player.name','player.firstName','player.lastName',
                                   'player.dateOfBirth','player.nationality','player.section','team.id']].rename(
    columns={'player.id':'player_id','player.name':'player_name','player.firstName':'first_name','player.lastName':'last_name',
             'player.dateOfBirth':'dob','player.nationality':'nationality',
             'player.section':'section','team.id':'team_id'}
).drop_duplicates(subset=['player_id'])

# COMMAND ----------

print(dimension_player.columns)

# COMMAND ----------

# test
df_matches['utcDate'] = pd.to_datetime(df_matches['utcDate'])
dimension_time = pd.DataFrame()
dimension_time['datetime'] = df_matches['utcDate']
dimension_time['date'] = df_matches['utcDate'].dt.date
dimension_time['year'] = df_matches['utcDate'].dt.year
dimension_time['month'] = df_matches['utcDate'].dt.month
dimension_time['month_name'] = df_matches['utcDate'].dt.month_name()
dimension_time['day'] = df_matches['utcDate'].dt.day
dimension_time['day_name'] = df_matches['utcDate'].dt.day_name()
dimension_time['week'] = df_matches['utcDate'].dt.isocalendar().week
dimension_time['hour'] = df_matches['utcDate'].dt.hour
dimension_time['minute'] = df_matches['utcDate'].dt.minute
dimension_time['quarter'] = df_matches['utcDate'].dt.quarter
dimension_time['matchday'] = df_matches['matchday']

#dimension_time['date_key'] = dimension_time['datetime'].dt.strftime('%Y%m%d').astype(int)
dimension_time['date_key'] = dimension_time['datetime'].dt.strftime('%Y%m%d').astype('int64')
dimension_time = dimension_time.drop_duplicates(subset=['date_key'])
print(dimension_time.head(3))
dimension_time.dtypes

# COMMAND ----------

dimension_time = dimension_time[[
    'date_key',
    'date',
    'year',
    'quarter',
    'month',
    'month_name',
    'week',
    'day',
    'day_name',
    'hour',
    'minute'
]]
dimension_time = dimension_time.astype({
    'date_key': 'int64',
    'year': 'int64',
    'quarter': 'int64',
    'month': 'int64',
    'week': 'int64',
    'day': 'int64',
    'hour': 'int64',
    'minute': 'int64'
})
dimension_time.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Fact Tables

# COMMAND ----------

fact_matches = df_matches[['id','utcDate','matchday','homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','score.winner','score.duration','competition.id','season.id','area.id']].rename(
    columns={'id':'match_id','utcDate':'match_date','matchday':'matchweek','homeTeam.id':'home_team_id',
             'awayTeam.id':'away_team_id','score.fullTime.home':'home_score','score.fullTime.away':'away_score',
             'score.winner':'winner','score.duration':'duration','competition.id':'competition_id',
             'season.id':'season_id','area.id':'area_id'}
)

fact_matches['date_key'] = pd.to_datetime(fact_matches['match_date']).dt.strftime('%Y%m%d').astype('int64')
fact_matches = fact_matches.drop(columns=['match_date'])
print(fact_matches.columns)

# COMMAND ----------

# fact_team_match_stats 2 rows for each match, one for home and one for away.
home_df = fact_matches[['match_id','home_team_id','away_team_id',
                        'home_score','away_score','matchweek',
                        'date_key','competition_id','season_id','area_id']].copy()

home_df.columns = [
    'match_id',
    'team_id',
    'opponent_id',
    'goals_scored',
    'goals_conceded',
    'matchweek',
    'date_key',
    'competition_id',
    'season_id',
    'area_id'
]

away_df = fact_matches[['match_id','away_team_id','home_team_id',
                        'away_score','home_score','matchweek',
                        'date_key','competition_id','season_id','area_id']].copy()

away_df.columns = [
    'match_id',
    'team_id',
    'opponent_id',
    'goals_scored',
    'goals_conceded',
    'matchweek',
    'date_key',
    'competition_id',
    'season_id',
    'area_id'
]

fact_team_match_stats = pd.concat([home_df, away_df], ignore_index=True)

# COMMAND ----------

# Create Result column
fact_team_match_stats['result'] = fact_team_match_stats.apply(
    lambda x: 'W' if x.goals_scored > x.goals_conceded
    else ('L' if x.goals_scored < x.goals_conceded else 'D'),
    axis=1
)

# Create Points column
fact_team_match_stats['points'] = fact_team_match_stats['result'].map({
    'W': 3,
    'D': 1,
    'L': 0
})

# Check
print(fact_team_match_stats.head())
print(fact_team_match_stats.shape)

# COMMAND ----------

team_comp_season = df_matches[['homeTeam.id','competition.id','season.id']].rename(
    columns={'homeTeam.id':'team_id','competition.id':'competition_id','season.id':'season_id'}
).drop_duplicates()

fact_player_stats = df_top_scorers[['player.id','team.id','playedMatches','goals','assists','penalties']].rename(
    columns={'player.id':'player_id','team.id':'team_id','playedMatches':'matches_played'}
)

fact_player_stats = fact_player_stats.merge(team_comp_season, on='team_id', how='left')

# COMMAND ----------

fact_standings = df_standings[['team.id','position','points','won','draw','lost','goalsFor','goalsAgainst','goalDifference','team.crest']].rename(
    columns={'team.id':'team_id','team.crest':'team_crest_url'}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## So now we have: 
# MAGIC ### Dimension Tables:
# MAGIC dimension_team
# MAGIC dimension_competition
# MAGIC dimension_season
# MAGIC dimension_area
# MAGIC dimension_player
# MAGIC
# MAGIC ### Fact Tables:
# MAGIC fact_matches
# MAGIC fact_player_stats
# MAGIC fact_standings

# COMMAND ----------

# For player
print(dimension_player.info())
print(dimension_player.head())

# For season
print(dimension_season.info())
print(dimension_season.head())

# COMMAND ----------

# Create Spark DataFrames from Pandas
spark_dimension_team = spark.createDataFrame(dimension_team)
spark_dimension_competition = spark.createDataFrame(dimension_competition)
spark_dimension_season = spark.createDataFrame(dimension_season)
spark_dimension_area = spark.createDataFrame(dimension_area)
spark_dimension_player = spark.createDataFrame(dimension_player)
spark_dimension_time = spark.createDataFrame(dimension_time)

spark_fact_team_match_stats = spark.createDataFrame(fact_team_match_stats)
spark_fact_matches = spark.createDataFrame(fact_matches)
spark_fact_player_stats = spark.createDataFrame(fact_player_stats)
spark_fact_standings = spark.createDataFrame(fact_standings)

# COMMAND ----------

# Save as tables on Catalog
spark_dimension_team.write.format("delta").mode("overwrite").saveAsTable("dimension_team")
spark_dimension_competition.write.format("delta").mode("overwrite").saveAsTable("dimension_competition")
spark_dimension_season.write.format("delta").mode("overwrite").saveAsTable("dimension_season")
spark_dimension_area.write.format("delta").mode("overwrite").saveAsTable("dimension_area")
spark_dimension_player.write.format("delta").mode("overwrite").saveAsTable("dimension_player")
spark_dimension_time.write.format("delta").mode("overwrite").saveAsTable("dimension_time")

spark_fact_team_match_stats.write.format("delta").mode("overwrite").saveAsTable("fact_team_match_stats")
spark_fact_matches.write.format("delta").mode("overwrite").saveAsTable("fact_matches")
spark_fact_player_stats.write.format("delta").mode("overwrite").saveAsTable("fact_player_stats")
spark_fact_standings.write.format("delta").mode("overwrite").saveAsTable("fact_standings")

# COMMAND ----------

spark.sql("SHOW TABLES").show()