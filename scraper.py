import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import psycopg2
from config import config
import json

def write_odi_full_scorecard(cursor):
    pages = []
    start_of_scorecards = 200
    end_of_scorecards = 201

    batters_columns = ['Player', 'Dismissal', 'Runs', 'BallsFaced', 'Fours', 'Sixes', 
                    'StrikeRate', 'Date', 'Location', 'Result', 'Innings', 
                    'Team' , 'BatSideRR', 'BatSideWicksLost', 'BatSideScore', 'Win']
    bowlers_columns = ['Player', 'Overs', 'Maidens', 'Runs', 'Wickets', 'EconRate', 
                    'Date', 'Location', 'Result', 'Innings', 'Team', 
                    'BatSideRR', 'BatSideWicketsLost', 'BatSideScore', 'Win']
    batters_combined_df = pd.DataFrame(columns=batters_columns)
    bowlers_combined_df = pd.DataFrame(columns=bowlers_columns)

    for i in range(start_of_scorecards,end_of_scorecards+1):
        pages.append(str(i).zfill(4))

    counter = 0
    for i in pages:
        # Getting the html from beautiful soup
        source = requests.get(f'http://www.howstat.com/cricket/Statistics/Matches/MatchScorecard_ODI.asp?MatchCode={i}').text
        soup = BeautifulSoup(source, 'lxml')
        # print(soup)

        # # Getting all of the data that repeats
        repeatables = np.array([item.text.strip() for item in soup.find_all(class_="TextBlack8")])
        date, location, result, rr_inn_1, rr_inn_2 = repeatables[[0,1,4,6,8]]
        rr_inn_1 = float(rr_inn_1.split('@')[1].split('rpo')[0].strip())
        rr_inn_2 = float(rr_inn_2.split('@')[1].split('rpo')[0].strip())
            
        repeatables_2 = np.array([item.text.strip() for item in soup.find_all(class_="TextBlackBold8")])
        team_1, team_1_rr_wicks, team_1_runs, team_2, team_2_rr_wicks, team_2_runs = repeatables_2[[7, 15, 16, 25, 33, 34]]

        try: 
            team_1_wicks_lost = int(team_1_rr_wicks.split('\r')[0].split('wickets')[0].strip())
        except ValueError:
            team_1_wicks_lost = 10
        team_1_rr = float(team_1_rr_wicks.split('@')[1].split('rpo')[0].strip())
        team_1_runs = int(team_1_runs)

        team_2 = team_2.split('\xa0')[0]
        try: 
            team_2_wicks_lost = int(team_2_rr_wicks.split('\r')[0].split('wickets')[0].strip())
        except ValueError:
            team_2_wicks_lost = 10
        team_2_rr = float(team_2_rr_wicks.split('@')[1].split('rpo')[0].strip())
        team_2_runs = int(team_2_runs)

        # # Determining the winner of each match (0-loss, 1-win, 2-draw)
        if result.split(" ")[0] == "Sri":
            winner = "Sri Lanka"
        elif result.split(" ")[0] == "New":
            winner = "New Zealand"
        elif result.split(" ")[0] == "South":
            winner = "South Africa"
        elif result.split(" ")[0] == "West":
            winner = "West Indies"
        else:
            winner = result.split(" ")[0]

        if winner == team_1:
            team_1_win = 1
            team_2_win = 0
        elif winner == team_2:
            team_1_win = 0
            team_2_win = 1
        else:
            team_1_win = 2
            team_2_win = 2

        # Getting all the non-repeatable data
        # Making a list of the players
        players = np.array([item.text for item in soup.find_all(class_="LinkOff")])
        # print(players)
        players = players[3:-2]
        players_team_1 = list(players[0:11])
        players_team_2 = [x for x in players if x not in players_team_1]
        players_team_2 = list(set(players_team_2))

        # Grabbing all of the data from the html
        tds = [item.text.strip() for item in soup.find('table').find_all('table')[4].find_all('table')[1].find_all('td')]
        tds = np.array(list(map(lambda x: x.replace('\x86', '').replace('*',''),tds)))

        # Grabing all of the player scorecard info
        player_scorecards = []
        used_index = []
        for player in players:
            indices = np.where(tds==player)[0]
            if len(indices)==1:
                player_scorecards.append([tds[indices][0], tds[indices+1][0], 
                                        tds[indices+2][0], tds[indices+3][0], 
                                        tds[indices+4][0], tds[indices+5][0], 
                                        tds[indices+6][0]])
                used_index.append(indices[0])
            elif len(indices)==2 and indices[0] not in used_index:
                player_scorecards.append([tds[indices][0], tds[indices+1][0], 
                                        tds[indices+2][0], tds[indices+3][0], 
                                        tds[indices+4][0], tds[indices+5][0], 
                                        tds[indices+6][0]])
                used_index.append(indices[0])
            else:
                player_scorecards.append([tds[indices][1], tds[indices+1][1], 
                                        tds[indices+2][1], tds[indices+3][1], 
                                        tds[indices+4][1], tds[indices+5][1], 
                                        tds[indices+6][1]])

        # Appending the date, location, and result for each player
        for item in player_scorecards:
            item.append(date)
            item.append(location)
            item.append(result)

        # Isolating the batters and bowlers from the scorecard
        batters = []
        bowlers = []

        for item in player_scorecards:
            try:
                float(item[1])
                bowlers.append(item)
            except ValueError:
                batters.append(item)

        # Adding the repeat data info for the batsmen
        for batter in batters[0:11]:
            batter.append(1)
            batter.append(team_1)
            batter.append(team_1_rr)
            batter.append(team_1_wicks_lost)
            batter.append(team_1_runs)
            batter.append(team_1_win)

        for batter in batters[11:]:
            batter.append(2)
            batter.append(team_2)
            batter.append(team_2_rr)
            batter.append(team_2_wicks_lost)
            batter.append(team_2_runs)
            batter.append(team_2_win)

        # Removing wickets taken as % of team wickets
        for x in bowlers:
            del x[6]

        # Adding team name to bowler
        for bowler in bowlers:
            if bowler[0] in players_team_1:
                bowler.append(2)
                bowler.append(team_1)
                bowler.append(team_2_rr)
                bowler.append(team_2_wicks_lost)
                bowler.append(team_2_runs)
                bowler.append(team_1_win)
            elif bowler[0] in players_team_2:
                bowler.append(1)
                bowler.append(team_2)
                bowler.append(team_1_rr)
                bowler.append(team_1_wicks_lost)
                bowler.append(team_1_runs)
                bowler.append(team_2_win)
        
        batters_df = pd.DataFrame(batters, columns=batters_columns)
        bowlers_df = pd.DataFrame(bowlers, columns=bowlers_columns)

        # Appending each dataframe to the combined dataframe
        batters_combined_df = batters_combined_df.append(batters_df)
        bowlers_combined_df = bowlers_combined_df.append(bowlers_df)

        # print(batters_combined_df.to_dict(orient='records'))
        batters_json = batters_combined_df.to_dict(orient='records')
        bowlers_json = bowlers_combined_df.to_dict(orient='records')
        
        cursor.execute("INSERT INTO fullscorecard (match_id, batting, bowling) VALUES (%s, %s, %s)", (i, json.dumps(batters_json), json.dumps(bowlers_json)))

        # Used to track errors
        counter += 1
    return counter

if __name__ == '__main__':
    """ Connect to the PostgreSQL database server """
    conn = None
    match_counter = 0
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        # create a cursor
        cur = conn.cursor()

        # send cursor for data insertion
        match_counter = write_odi_full_scorecard(cur)
       
     # close the communication with the PostgreSQL
        cur.close()
    except psycopg2.DatabaseError as error:
        print(error)
    except Exception as Err:
        print(Err)
        error_page = str(200+match_counter-1).zfill(4)
        print(f'http://www.howstat.com/cricket/Statistics/Matches/MatchScorecard_ODI.asp?MatchCode={error_page}')
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')