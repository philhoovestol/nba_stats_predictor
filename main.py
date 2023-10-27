import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from tqdm import tqdm

from requests.exceptions import ReadTimeout

import traceback

import time
import os

QUERY_API_FOR_MORE_BOX_SCORES = False  # will do this no matter what if box_scores.csv doesn't exist


got_new_games = False

if not os.path.exists("games.csv"):
    print("Querying the NBA API for game data...")

    # Query the NBA API for as many games as possible
    gamefinder = leaguegamefinder.LeagueGameFinder(
        league_id_nullable='00',
        season_type_nullable='Regular Season',
    )
    games = gamefinder.get_data_frames()[0]
    got_new_games = True
    print(f"Number of game received by LeagueGameFinder: {games.shape[0]}")
    print("Saving new games to games.csv...")
    games.to_csv('games.csv', index=False)

else:
    games = pd.read_csv("games.csv")
    print(f"Read {games.shape[0]} games from CSV file")

game_ids = games['GAME_ID'].tolist()

print(f"Min game date: {games['GAME_DATE'].min()}")
print(f"Max game date: {games['GAME_DATE'].max()}")

print("Continue to pulling box scores? (y/n)")
if input() != 'y':
    exit()

if QUERY_API_FOR_MORE_BOX_SCORES or not os.path.exists("box_scores.csv"):
    # Create an empty list to store the box score data
    box_scores = []

    time.sleep(1)

    START_INDEX = 6848  # get from log output if code fails (Too many requests to the NBA API. Stopping at game ID [next START_INDEX]])

    print("Querying the NBA API for box score data...")

    try:
        # Loop through each game ID and get the box score data for each player
        for g_i, game_id in enumerate(tqdm(game_ids)):
            if g_i < START_INDEX:
                continue

            # convert game_id to string of length 10 (padding zeroes at the beginning if necessary)
            game_id = str(game_id).zfill(10)

            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                end_period=10,
                end_range=28800,
                range_type=0,
                start_period=1,
                start_range=0
            )
            box_scores.append(box_score.get_data_frames()[0])
            time.sleep(2)
    except ReadTimeout:
        print(f"Too many requests to the NBA API. Stopping at game ID {g_i}: {game_id}")
        print(f"Exception: {traceback.format_exc()}")

    # Concatenate all of the box score data into a single dataframe
    box_scores_df = pd.concat(box_scores)

    # Drop any duplicate rows
    box_scores_df.drop_duplicates(inplace=True)

    # Reset the index
    box_scores_df.reset_index(drop=True, inplace=True)

    print(f"Number of new [player/game] rows created from API responses: {box_scores_df.shape[0]}")

    # Save the dataframe to a CSV file
    print("Appending new box scores to box_scores.csv...")
    box_scores_df.to_csv('box_scores.csv', mode="a", index=False, header=False)

# Pull up complete box score data
box_scores_df = pd.read_csv("box_scores.csv")

# Display the resulting dataframe
print(f"Number of total [player/game] rows pulled from box_scores.csv: {box_scores_df.shape[0]}")
print(f"columns: {box_scores_df.columns}")

print("Continue to calculating opponent prior averages? (y/n)")
if input() != 'y':
    exit()


# Define a function to calculate the average value of a stat for opposing players coming into a game
def get_opp_avg(game_id, team_id, stat):
    # Get the opposing team ID
    team_ids = box_scores_df.loc[box_scores_df['GAME_ID'] == game_id, 'TEAM_ID'].unique()
    if len(team_ids) == 1:
        # If the team in the row is the only team that played in the game, return NaN
        return float('nan')
    else:
        # Get the opposing team ID
        opposing_team_id = [t_id for t_id in team_ids if t_id != team_id][0]
        # Get all players who played for the opposing team in the game
        opposing_player_ids = box_scores_df.loc[(box_scores_df['GAME_ID'] == game_id) & (box_scores_df['TEAM_ID'] == opposing_team_id), 'PLAYER_ID'].unique()
        # Get the average value of the stat for each opposing player in the current season leading up to the game
        opposing_player_avgs = []
        for opposing_player_id in opposing_player_ids:

            season_id = games.loc[games['GAME_ID'] == game_id, 'SEASON_ID'].iloc[0]
            game_date = games.loc[games['GAME_ID'] == game_id, 'GAME_DATE'].iloc[0]
            
            # Get all game IDs for the current season leading up to the game that the opposing player played in
            game_ids = games.loc[(games['GAME_DATE'] < game_date) & (games['SEASON_ID'] == season_id) & (games['TEAM_ID'] == opposing_team_id), 'GAME_ID'].tolist()
            opposing_player_games = box_scores_df.loc[(box_scores_df['GAME_ID'].isin(game_ids)) & (box_scores_df['TEAM_ID'] == opposing_team_id) & (box_scores_df['PLAYER_ID'] == opposing_player_id)]
            
            if opposing_player_games.empty:
                # If the opposing player has not played any games in the current season leading up to the game, skip them
                continue
            else:
                opposing_player_avg = opposing_player_games[stat].mean()
                opposing_player_avgs.append(opposing_player_avg)
        if not opposing_player_avgs:
            # If no opposing players have played any games in the current season leading up to the game, return NaN
            return float('nan')
        else:
            # Return the average value of the stat for all opposing players in the current season leading up to the game
            return sum(opposing_player_avgs) / len(opposing_player_avgs)


print("Calculating opponent prior averages...")

# Loop through each non-id numerical stat in box_scores_df (excluding MIN)
cols_to_add_opp_avg = [col for col in box_scores_df.columns if
             col not in ['GAME_ID', 'TEAM_ID', 'PLAYER_ID', 'MIN'] and box_scores_df[col].dtype != 'object']
print(f"for these columns: {cols_to_add_opp_avg}")
pbar = tqdm(total=len(cols_to_add_opp_avg) * box_scores_df.shape[0])
for stat in cols_to_add_opp_avg:

    avgs = []

    def get_opp_avg_wrapper(row):
        # Calculate the value for the new column
        opp_avg = get_opp_avg(row['GAME_ID'], row['TEAM_ID'], stat)
        avgs.append(opp_avg)
        pbar.update(1)

    box_scores_df.apply(get_opp_avg_wrapper, axis=1)

    # Add the new column to the row
    box_scores_df[f"{stat}_opp_avg"] = avgs
pbar.close()

# Save the dataframe to a CSV file
print("Saving box scores with opponent prior averages to CSV file...")
box_scores_df.to_csv('box_scores_with_opp_prior_avg.csv', index=False)





