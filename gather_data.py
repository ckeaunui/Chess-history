import requests
from datetime import datetime
from tqdm import tqdm
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


def gather_data(username):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    }

    stats_url = f"https://api.chess.com/pub/player/{username}/stats"
    player_stats = requests.get(stats_url, headers=headers).json()

    # Get all games since creation date and save to pd dataframe
    all_games = []
    todays_date = datetime.now()
    monthly_data_urls = \
    requests.get(f"https://api.chess.com/pub/player/{username}/games/archives", headers=headers).json()['archives']
    for url in tqdm(monthly_data_urls, position=0, leave=True):
        games = requests.get(url, headers=headers).json()['games']
        all_games += games
    print(f"Found {len(all_games)} Games")

    # Build df
    df = pd.DataFrame.from_dict(all_games, orient='columns')
    return df


# Pull the important information out of the pgn column and return it in a dict
def pgn_to_dict(pgn) -> dict:
    pgn_data = {}
    idx_to_col = {
        2: 'start_date',
        6: 'result',
        9: 'ECO',
        16: 'termination',
        17: 'start_time',
        18: 'end_date',
        19: 'end_time',
        22: 'moves'
    }

    for i, row in enumerate(pgn.split('\n')):
        if i in idx_to_col.keys():
            if i == 22:
                pgn_data[idx_to_col[i]] = row
            else:
                data = row.split("\"")[1]
                pgn_data[idx_to_col[i]] = data
    return pgn_data


# Turn the string of pgn moves into a json format
def game_notation_to_json(moves):
    moves_json = {}
    for i, line in enumerate(moves.split(']} ')):
        if i + 1 == len(moves.split(']}')):
            break

        w_or_b = 'white' if i % 2 == 0 else 'black'
        move_no, move, clock = line.replace('{[%clk ', '').replace('..', '').replace('. ', ' ').split(' ')
        move_no = int(move_no)
        ply_data = {'move': move, 'clock': clock}

        if move_no not in moves_json.keys():
            moves_json[move_no] = {'white': {'move': None, 'clock': None},
                                   'black': {'move': None, 'clock': None}, }

        moves_json[move_no][w_or_b] = ply_data
    return moves_json





def clean_data(df, username):
    # Remove non-traditional games (Fischer random, bughouse, chess960, ...)
    df = df.query("rules == 'chess'")
    df = df.rename(columns={"start_time": "start_time_utx", "end_time": "end_time_utx"})

    # Expand all nested data data on one row
    for index, row in tqdm(df.iterrows(), position=0, leave=True):
        # Expand accuracies column
        w_accuracy = None
        b_accuracy = None

        # If the game was analyzed, add accuracies
        if type(row['accuracies']) != float:
            w_accuracy = row['accuracies']['white']
            b_accuracy = row['accuracies']['black']

        if row['white']['username'] == username:
            df.at[index, 'my_accuracy'] = w_accuracy
            df.at[index, 'my_rating'] = row['white']['rating']
            df.at[index, 'my_result'] = row['white']['result']
            df.at[index, 'my_username'] = row['white']['username']
            df.at[index, 'my_color'] = 'white'

            df.at[index, 'opponent_accuracy'] = b_accuracy
            df.at[index, 'opponent_rating'] = row['black']['rating']
            df.at[index, 'opponent_result'] = row['black']['result']
            df.at[index, 'opponent_username'] = row['black']['username']
            df.at[index, 'opponent_color'] = 'black'

        elif row['black']['username'] == username:
            df.at[index, 'opponent_accuracy'] = w_accuracy
            df.at[index, 'opponent_rating'] = row['white']['rating']
            df.at[index, 'opponent_result'] = row['white']['result']
            df.at[index, 'opponent_username'] = row['white']['username']
            df.at[index, 'opponent_color'] = 'white'

            df.at[index, 'my_accuracy'] = b_accuracy
            df.at[index, 'my_rating'] = row['black']['rating']
            df.at[index, 'my_result'] = row['black']['result']
            df.at[index, 'my_username'] = row['black']['username']
            df.at[index, 'my_color'] = 'black'
        else:
            print(f"Username mismatch: {index}")
            continue

        # Expand pgn column
        pgn_data = pgn_to_dict(row['pgn'])
        for col_name, pgn_row in pgn_data.items():
            df.at[index, col_name] = pgn_row

        # Expand moves column
        moves_json = game_notation_to_json(pgn_data['moves'])

        # Check the game has move history
        if len(moves_json) > 0:
            df.at[index, 'moves'] = moves_json
            # df.at[index, 'opening'] = eco_codes[df.at[index, 'ECO']][0]
            # df.at[index, 'opening_moves'] = eco_codes[df.at[index, 'ECO']][1]
            ply = int(len(moves_json) * 2 - 1) if moves_json[len(moves_json)]['black']['move'] is None else int(
                len(moves_json) * 2)
            df.at[index, 'ply'] = int(ply)

    # Remove old / unwanted columns
    df = df.drop(columns=['accuracies', 'black', 'white', 'pgn', 'initial_setup'], axis=1)
    df = df.query("time_class != 'daily'")

    # Order by date
    df.sort_values('end_time_utx')
    return df


def save_as_json(df, path='', filename=''):
    df.to_json(path + filename)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)

    username = 'Spicy_Chris'
    df = gather_data(username)
    df = clean_data(df, username)
    print(df.head())
    save_as_json(df, 'chess-data.json')










# Clean data







"""
# Web scrape openings and their eco codes
eco_codes = {}
eco_url = "https://www.chessgames.com/chessecohelp.html"
eco_response = requests.get(eco_url, headers=headers)

soup = BeautifulSoup(eco_response.content, "html.parser")
for row in soup.find_all('tr'):
    eco = row.find_all('td')[0].get_text()
    opening_data = row.find_all('td')[1].get_text().split('\n')
    eco_codes[eco] = opening_data

print(eco_codes)

print(df.head())
# Save to json

"""
