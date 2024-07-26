import pandas as pd
import numpy as np

class fbrefStats:
    def __init__(self, url):
        self.url = url

    def clean_headers(self, df):
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        return df

    def create_url(self, current_url):
        comp_url = 'https://fbref.com' + current_url
        return comp_url

    def get_latest_season(self):
        # Extract competition table into a dataframe
        seasons = pd.read_html(self.url, extract_links='all')[0].iloc[:, 0:2]

        # Return the season url part for further queries
        url_part = seasons.iloc[:, 0][0][1]
        season_url = self.create_url(url_part)
        # Return the current season of the competition
        current_season = seasons.iloc[:, 0][0][0]
        # Return the name of the competition
        competition = seasons.iloc[:, 1][0][0]

        competition_dict = {'competition': competition, 'season': current_season, 'url': season_url}

        return competition_dict

    def get_fixtures(self):
        competition_dict = self.get_latest_season()
        season_url = competition_dict['url']
        season = competition_dict['season']
        competition = competition_dict['competition']

        fixture_url = season_url.rsplit('/', 1)[0] + '/schedule/'
        fixtures = pd.read_html(fixture_url, extract_links='body')[0]

        # Explode the dataframe with URL
        # URL value will be added as new row
        # Remove duplicated row on index for fixture dataframe
        fixtures_exp = fixtures.explode(list(fixtures.columns))
        fixtures_exp['Score'] = fixtures_exp['Score'].replace('', np.nan)
        fixtures_exp.dropna(subset=['Score'], inplace=True)
        fixtures = fixtures_exp.groupby(fixtures_exp.index).first()

        # Use duplicated row to define team id of home and away
        # And define the url and the match id
        id_columns = fixtures_exp.groupby(fixtures_exp.index).last()
        id_columns = id_columns.dropna(subset=['Score'])
        id_columns['url'] = [self.create_url(x) for x in id_columns['Match Report']]
        id_columns['match_id'] = [x[0].split('/en/matches/')[1] for x in id_columns['Match Report'].str.rsplit('/', n=1)]
        id_columns['home_id'] = [x[0].split('/en/squads/')[1] for x in id_columns['Home'].str.rsplit('/', n=1)]
        id_columns['away_id'] = [x[0].split('/en/squads/')[1] for x in id_columns['Away'].str.rsplit('/', n=1)]
        id_columns = id_columns.iloc[:, -4:]

        # Add the id columns to the fixture dataframe
        fixtures = pd.concat([fixtures, id_columns], axis=1).reset_index().drop(columns='index')

        # Rename column headers
        fixtures = self.clean_headers(fixtures)

        # Add column
        fixtures['competition'] = competition
        fixtures['season'] = season
        fixtures['inserted_timestamp'] = pd.to_datetime('today')

        # Column [Round] is not present for every competition. Fill the column with a default value if non is existent
        if 'round' not in fixtures.columns:
            #fixtures['round'] = 'Regular Season'
            fixtures.insert(loc=0, column='round', value='Regular Season')
        
        return fixtures

    def get_teams(self, df):
        # Create a dataframe with team_id and team name
        home = df[['home_id', 'home', 'inserted_timestamp']].rename(columns={'home': 'team_name', 'home_id': 'team_id'})
        away = df[['away_id', 'away', 'inserted_timestamp']].rename(columns={'away': 'team_name', 'away_id': 'team_id'})

        teams = pd.concat([home, away], axis=0)
        teams = teams.drop_duplicates(subset='team_id').copy().reset_index().drop(columns='index')
        teams['team_name'] = [x[0] for x in teams['team_name'].str.rsplit(' ')]

        return teams

    def transform_scores(self, df):
        scores = df.dropna(subset=['score']).copy()

        # Split the score
        scores['score_home'] = [x[0].split(" ")[-1] for x in df['score'].str.split('–')]
        scores['score_away'] = [x[1].split(" ")[0] for x in df['score'].str.split('–')]
        scores['score_home'] = scores['score_home'].astype(int)
        scores['score_away'] = scores['score_away'].astype(int)
        scores = scores.drop(columns=['score']).copy()

        # Label matches which had a Penalty Shootout
        scores['notes'] = np.where(scores['notes'].str.contains('penalty', regex=False) == True, 'Penalty Shootout', scores['notes'])

        # Rename columns
        scores_home = scores[['url', 'match_id', 'competition', 'season', 'round', 'wk', 'day', 'date', 'time',
                          'home_id', 'away', 'score_home', 'score_away', 'xg', 'xg.1', 'notes', 'inserted_timestamp']].copy()
        scores_away = scores[['url', 'match_id', 'competition', 'season', 'round', 'wk', 'day', 'date', 'time', 
                              'away_id', 'home', 'score_away', 'score_home', 'xg.1', 'xg', 'notes', 'inserted_timestamp']].copy()

        scores_home.rename(columns={'home_id': 'team_id', 'away': 'opponent', 'score_home': 'score', 
                            'score_away': 'score_opp', 'xg.1': 'xg_opp'}, inplace=True)

        scores_away.rename(columns={'away_id': 'team_id', 'home': 'opponent', 'away': 'team', 'score_home': 'score_opp', 
                                    'score_away': 'score', 'xg.1': 'xg', 'xg': 'xg_opp'}, inplace=True)

        # Add home venue for all games in df
        scores_home['venue'] = 'home'
        scores_away['venue'] = 'away'

        scores = pd.concat([scores_home, scores_away]).reset_index()

        scores['date'] = pd.to_datetime(scores['date'])
        # scores['wk'] = scores['wk'].replace({np.nan: 0}).astype(int)

        return scores


    def get_match_details(self, df):
        competition_dict = self.get_latest_season()
        season = competition_dict['season']
        competition = competition_dict['competition']
        
        # Initiate empty Dataframes
        shots = pd.DataFrame()
        gk_stats = pd.DataFrame()

        ### Extraction
        # Loop through last recorded games to extract match_details
        for i, row in df.iterrows():
            # Declare variables match_id and url
            match_id = df['match_id'][i]
            url = df['url'][i]
            home_team = df['home_id'][i]

            # Read html output from match url
            html_output = pd.read_html(url, extract_links='body')


            ### Extract goalkeeper statistics from both goalkeeper
            gk_columns = ['player', 'age', 'min', 'shots', 'goals',
            'saves', 'save_perc', 'psxg', 'launch_completion', 
            'launch_attempts', 'launch_comp_percentage', 
            'pass_attempt', 'throws', 'launch_percentage', 
            'pass_average_length', 'goalkicks', 'goalkicks_launched_percentage',
            'goalkicks_average_length', 'crosses', 'crosses_stopped', 
            'crosses_stopped_percentage', 'actions_outside_penaltyarea', 'actions_average_distance']
            
            # Create home goalkeeper dataframe and drop nation column
            gk1_exp = html_output[9].explode(list(html_output[9].columns)).drop(["('Unnamed: 1_level_0', 'Nation')"], axis=1, errors='ignore')
            gk1_output = gk1_exp.groupby(gk1_exp.index).first()
            
            # Create away goalkeeper dataframe and drop nation column
            gk2_exp = html_output[16].explode(list(html_output[16].columns)).drop(["('Unnamed: 1_level_0', 'Nation')"], axis=1, errors='ignore')
            gk2_output = gk2_exp.groupby(gk2_exp.index).first()

            # Combine home and away GK dataframes
            gk_all_output = pd.concat([gk1_output, gk2_output])
            gk_all_output.columns = gk_all_output.columns.map(lambda x: x[1])
            gk_all_output = gk_all_output.drop(columns=['Nation'])
            gk_all_output = gk_all_output.reset_index().drop(columns='index')
            gk_all_output = gk_all_output.set_axis(gk_columns, axis=1)
            
            # Add match_id and append to overall dataframe
            gk_all_output['match_id'] = match_id
            gk_stats = pd.concat([gk_stats, gk_all_output]).reset_index().drop(columns=['index'])
            
            # Define home and away goalkeeper name for shots
            home_goalkeeper = gk1_output.iloc[0, 0]
            away_goalkeeper = gk2_output.iloc[0, 0]

            ### Extract shot statistics from match_detail
            shot_columns = ['minute', 'player', 'squad', 'xg', 'psxg', 
                            'outcome', 'distance', 'bodypart', 'notes', 
                            'assist_player1', 'assist1', 'assist_player2', 'assist2']
            shot_exp = html_output[-3].explode(list(html_output[-3].columns))
            shot_exp[shot_exp.columns[1]] = shot_exp[shot_exp.columns[1]].replace('', np.nan)
            shot_exp[shot_exp.columns[-2]] = shot_exp[shot_exp.columns[-2]].replace('', np.nan)
            shot_exp[shot_exp.columns[-4]] = shot_exp[shot_exp.columns[-4]].replace('', np.nan)
            shot_exp.dropna(subset=[shot_exp.columns[1]], inplace=True)
            shot_output = shot_exp.groupby(shot_exp.index).first()
            shot_output = shot_output.set_axis(shot_columns, axis=1)
            shot_output.dropna(subset=['minute'], inplace=True)

            # Extract the team id
            shot_ids = shot_exp.groupby(shot_exp.index).last()
            shot_output['team_id'] = [x[0].split('/en/squads/')[1] for x in shot_ids.iloc[:, 2].str.rsplit('/', n=1)]

            # Add match_id and goalkeeper  column
            shot_output['match_id'] = match_id
            shot_output['goalkeeper'] = np.where(shot_output['team_id'] == home_team, away_goalkeeper, home_goalkeeper)

            # Add transformed rows to shot dataframe
            shots = pd.concat([shots, shot_output]).reset_index().drop(columns=['index'])

        ### Cleaning
        # Remove Added time from the Minute column 45/90 is the max
        shots['minute'] = [x[0] for x in shots['minute'].astype(str).str.split('+')]
        shots['minute'] = shots['minute'].astype(float).astype(int)

        # Remove Penalty note from Player and add to Notes column
        notes_list = []
        for i in range(len(shots)):
            if shots.loc[i]['player'].rsplit("(")[-1] == 'pen)':
                notes_list.append('Penalty')
            else:
                notes_list.append(shots.loc[i]['notes'])

        shots['notes'] = notes_list
        shots['player'] = [x[0] for x in shots['player'].str.rsplit("(")] # Player
        shots['competition'] = competition
        shots['season'] = season

        # Add competition to goalkeeper stats
        gk_stats['competition'] = competition
        gk_stats['season'] = season

        return shots, gk_stats