import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import psycopg2
from config import config
import json
from time import gmtime, strftime

def writeSQL(conn, sql_query, data):
    # start at the beginning, select the first text field
    cur = conn.cursor()
    cur.execute(sql_query, data)
    conn.commit()
    count = cur.rowcount
    cur.close()
    return count

def readSQL(conn, sql_query, data):
    cur = conn.cursor()
    cur.execute(sql_query, data)
    row = cur.fetchone()
    return row

def write_odi_full_scorecard(conn):
    pages = []
    # count=0
    start_of_scorecards = 200
    end_of_scorecards = 300

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
        try:
            # Getting the html from beautiful soup
            source = requests.get(f'http://www.howstat.com/cricket/Statistics/Matches/MatchScorecard_ODI.asp?MatchCode={i}').text
            soup = BeautifulSoup(source, 'lxml')
            # print(soup)

            # # Getting the series information
            banner_repeatables = np.array([item.text.strip() for item in soup.find_all(class_="LinkBlack2")])
            series_name, venue = banner_repeatables[[0,1]]
            name_of_match = banner_repeatables[[0,1]]

            # # Getting all of the data that repeats
            repeatables = np.array([item.text.strip() for item in soup.find_all(class_="TextBlack8")])
            date, location, toss, result, player_of_match, rr_inn_1, rr_inn_2 = repeatables[[0,1,3,4,5,6,8]]
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
            # print(players_team_1)
            # print(players_team_2)

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
            
            #team table insertion
            try:
                status=True
                query= """
                INSERT INTO TEAM (name, status) 
                SELECT %s, %s WHERE NOT EXISTS 
                ( SELECT id FROM TEAM WHERE NAME = %s)""" 
                params = (team_1, status, team_1)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("team table err")
                print(Err)  

            #country table insertion
            try:
                status=True
                query= """
                INSERT INTO COUNTRY (name, status) 
                SELECT %s, %s WHERE NOT EXISTS 
                ( SELECT id FROM COUNTRY WHERE NAME = %s)""" 
                params = (team_1, status, team_1)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("country table err")
                print(Err)   

            #match type table insertion
            try:
                query= """
                INSERT INTO MATCHTYPE (TYPE, NAME, START_DATE, END_DATE, VENUE, G_ID, CREATED, MODIFIED) 
                SELECT 'SERIES', %s, null, null, %s, null, %s, %s WHERE NOT EXISTS 
                ( SELECT id FROM MATCHTYPE WHERE NAME = %s)""" 
                params = (series_name, venue, strftime("%Y-%m-%d %H:%M:%S", gmtime()), strftime("%Y-%m-%d %H:%M:%S", gmtime()), series_name)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("match type table err")
                print(Err)   

            #players table insertion
            try:
                query_team1= """
                SELECT ID FROM TEAM WHERE NAME = %s
                """
                params = (team_1,)
                row_id_team1 = readSQL(conn, query_team1, params)
                for player in players_team_1:
                    query= """
                    INSERT INTO PLAYER (NAME, DOB, HISTORY, AGE, TYPE, BATTING_STYLE, BOWLING_STYLE, TEAM_ID) 
                    SELECT %s, null, null, null, null, null, null, %s WHERE NOT EXISTS 
                    ( SELECT ID FROM PLAYER WHERE NAME = %s)"""
                    params = (player, row_id_team1, player)
                    writeSQL(conn, query, params)
                query_team2= """
                SELECT ID FROM TEAM WHERE NAME = %s
                """
                params = (team_2,)
                row_id_team2 = readSQL(conn, query_team2, params)
                for player in players_team_2:
                    query= """
                    INSERT INTO PLAYER (NAME, DOB, HISTORY, AGE, TYPE, BATTING_STYLE, BOWLING_STYLE, TEAM_ID) 
                    SELECT %s, null, null, null, null, null, null, %s WHERE NOT EXISTS 
                    ( SELECT ID FROM PLAYER WHERE NAME = %s)"""
                    params = (player, row_id_team2, player)
                    writeSQL(conn, query, params)   
            except Exception as Err:
                print("players table err")
                print(Err)   

            # squad table insertion    
            try:
                query= """
                SELECT TEAM_SQUAD_1, TEAM_SQUAD_2 FROM squad ORDER BY ID desc limit 1
                """
                row_id = readSQL(conn, query, None)
                if row_id is None:
                    t_id_1 = 1
                    t_id_2 = 2
                else:
                    t_id_1 = row_id[0] + 2
                    t_id_2 = row_id[1] + 2

                query= """
                    INSERT INTO SQUAD (TEAM_SQUAD_1, TEAM_SQUAD_2) 
                    SELECT %s, %s WHERE NOT EXISTS 
                    ( SELECT ID FROM SQUAD WHERE TEAM_SQUAD_1 = %s AND TEAM_SQUAD_2 = %s)"""
                params = (t_id_1, t_id_2, t_id_1, t_id_2)
                writeSQL(conn, query, params)
                SELECTED = True
                for player in players_team_1:
                    query_team1= """
                    SELECT ID FROM PLAYER WHERE NAME = %s
                    """
                    params = (player,)
                    row_id_player = readSQL(conn, query_team1, params)
                    query_team_squad= """
                        INSERT INTO TEAMSQUAD (PLAYER_ID, TEAM_ID, T_ID_1, T_ID_2, SELECTED) 
                        SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS 
                        ( SELECT ID FROM TEAMSQUAD WHERE PLAYER_ID = %s and TEAM_ID = %s AND T_ID_1 = %s AND T_ID_2 = %s AND SELECTED = %s)"""
                    params = (row_id_player, row_id_team1, t_id_1, None, SELECTED, row_id_player, row_id_team1, t_id_1, None, SELECTED)
                    writeSQL(conn, query_team_squad, params)
                for player in players_team_2:
                    query_team2= """
                    SELECT ID FROM PLAYER WHERE NAME = %s
                    """
                    params = (player,)
                    row_id_player = readSQL(conn, query_team2, params)
                    query_team_squad= """
                        INSERT INTO TEAMSQUAD (PLAYER_ID, TEAM_ID, T_ID_1, T_ID_2, SELECTED) 
                        SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS 
                        ( SELECT ID FROM TEAMSQUAD WHERE PLAYER_ID = %s and TEAM_ID = %s AND T_ID_1 = %s AND T_ID_2 = %s AND SELECTED = %s)"""
                    params = (row_id_player, row_id_team2, None, t_id_2, SELECTED, row_id_player, row_id_team1, None, t_id_2, SELECTED)
                    writeSQL(conn, query_team_squad, params)
            except Exception as Err:
                print("squad table err")
                print(Err)   

            # score table insertion    
            try:
                query= """
                SELECT SCORECARD_ID FROM SCORE ORDER BY ID desc limit 1
                """
                row_score_id = readSQL(conn, query, None)
                if row_score_id is None:
                    s_id = 1
                else:
                    s_id = row_score_id[0] + 1
                query= """
                    INSERT INTO SCORE (SCORECARD_ID) 
                    SELECT %s WHERE NOT EXISTS 
                    ( SELECT ID FROM SCORE WHERE SCORECARD_ID = %s)"""
                params = (s_id,s_id)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("score table err")
                print(Err)   

            # RESULT table insertion    
            try:
                #team table insertion
                query= """
                INSERT INTO RESULT (DESCRIPTION) 
                VALUES
                (%s)
                """ 
                params = (result,)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("result table err")
                print(Err)   

            # Match table insertion    
            try:
                query_toss= """
                SELECT ID FROM TEAM WHERE NAME = %s
                """
                params = (toss,)
                row_id_toss = readSQL(conn, query_toss, params)
                query_squad= """
                SELECT ID FROM SQUAD ORDER BY ID desc limit 1
                """
                row_id_squad = readSQL(conn, query_squad, None)
                query_format= """
                SELECT ID FROM FORMAT WHERE NAME = 'ODI'
                """
                row_id_format = readSQL(conn, query_format, None)
                query_matchtype= """
                SELECT ID FROM MATCHTYPE WHERE NAME = %s
                """
                params = (series_name,)
                row_id_matchtype = readSQL(conn, query_matchtype, params)
                query_score= """
                SELECT ID FROM SCORE ORDER BY ID desc limit 1
                """
                row_id_score = readSQL(conn, query_score, None)
                query_result= """
                SELECT ID FROM RESULT ORDER BY ID desc limit 1
                """
                row_id_result = readSQL(conn, query_result, None)
                query_mom= """
                SELECT ID FROM PLAYER WHERE NAME = %s
                """
                params = (player_of_match,)
                row_id_mom = readSQL(conn, query_mom, params)
                status=False
                # print(date)
                ## match insertion
                query= """
                    INSERT INTO MATCH (NAME, DATE, LOCAL, VENUE, TOSS, STATUS, SQUAD_ID, FORMAT_ID, MATCHTYPE_ID, SCORE_ID, RESULT_ID, COUNTRY_ID, MOM ) 
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                params = (name_of_match[0], None, None, location, row_id_toss, status, row_id_squad, row_id_format,  row_id_matchtype, row_id_score, row_id_result, None, row_id_mom)
                writeSQL(conn, query, params)
            except Exception as Err:
                print("match table err")
                print(Err)   

            #full scorecard insertion
            try:
                query_match= """
                SELECT ID FROM MATCH ORDER BY ID desc limit 1
                """
                row_id_match = readSQL(conn, query_match, None)
                query_fullscore= """
                    INSERT INTO FULLSCORECARD (match_id, batting, bowling ) 
                    VALUES
                    (%s, %s, %s)
                    """
                params = (row_id_match, json.dumps(batters_json), json.dumps(bowlers_json))
                writeSQL(conn, query_fullscore, params)
                # cursor.execute("INSERT INTO fullscorecard (match_id, batting, bowling) VALUES (%s, %s, %s)", (i, json.dumps(batters_json), json.dumps(bowlers_json)))
            except Exception as Err:
                print("fullscore card table err")
                print(Err)   
            # Used to track errors
            counter += 1
        except:
            error_page = str(200+counter-1).zfill(4)
            print(f'http://www.howstat.com/cricket/Statistics/Matches/MatchScorecard_ODI.asp?MatchCode={error_page}')    

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
 
        # send conn for data insertion
        write_odi_full_scorecard(conn)
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')