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
        fixtures = pd.read_html(fixture_url)[0]
        fixtures = fixtures.iloc[:, :-5]
        urls = pd.read_html(fixture_url, extract_links='all')[0].iloc[:, -2].rename('url')
        fixtures = pd.concat([fixtures, urls], axis=1)
        fixtures = fixtures.dropna(subset=['Score']).copy()

        # Rename column headers
        fixtures = self.clean_headers(fixtures)

        # Add column
        fixtures['competition'] = competition
        fixtures['season'] = season

        # Extract URL to matches
        fixtures['url'] = [self.create_url(x[1]) for x in fixtures['url']]
        
        return fixtures

    def transform_scores(self, df):
        scores = df.dropna(subset=['score']).copy()

        # Get Match ID
        scores.loc[:,'match_id'] = [x[0].split('https://fbref.com/en/matches/')[1] for x in scores['url'].str.rsplit('/', n=1)]
        scores.set_index('match_id', inplace=True)

        # Split the score
        scores['score_home'] = [x[0] for x in scores['score'].str.split('–')]
        scores['score_away'] = [x[1] for x in scores['score'].str.split('–')]
        scores['score_home'] = scores['score_home'].astype(int)
        scores['score_away'] = scores['score_away'].astype(int)
        scores.drop(columns=['score'], inplace=True)

        # Rename columns
        scores = scores[['url', 'competition', 'season', 'wk', 'day', 'date', 'time',  'home', 'away', 'score_home', 'score_away', 'xg', 'xg.1']].copy()
        scores_away = scores[['url', 'competition', 'season', 'wk', 'day', 'date', 'time',  'away', 'home', 'score_away', 'score_home', 'xg.1', 'xg']].copy()

        scores.rename(columns={'home': 'team', 'away': 'opponent', 'score_home': 'score', 
                            'score_away': 'score_opp', 'xg.1': 'xg_opp'}, inplace=True)

        scores_away.rename(columns={'home': 'opponent', 'away': 'team', 'score_home': 'score_opp', 
                                    'score_away': 'score', 'xg.1': 'xg', 'xg': 'xg_opp'}, inplace=True)

        # Add home venue for all games in df
        scores['venue'] = 'home'
        scores_away['venue'] = 'away'

        scores = pd.concat([scores, scores_away]).reset_index()

        scores['date'] = pd.to_datetime(scores['date'])
        df['wk'].replace({np.nan: 0}).astype(int)

        return scores


    def get_match_details(self, df):
        competition_dict = self.get_latest_season()
        season = competition_dict['season']
        competition = competition_dict['competition']
        
        # Select only home games to reduce to single games
        df = df[df['venue'] == 'home']

        # Define the last current date and filter dataframe
        last_date = np.max(df['date'])
        filtered_df = df[df['date'] == last_date].reset_index().drop(columns='index')
        
        # Initiate empty Dataframes
        shots = pd.DataFrame()
        gk_stats = pd.DataFrame()

        ### Extraction
        # Loop through last recorded games to extract match_details
        for i in range(len(filtered_df)):
            # Declare variables match_id and url
            match_id = filtered_df['match_id'][i]
            url = filtered_df['url'][i]

            # Read html output from match url
            html_output = pd.read_html(url)

            # Extract shot statistics from match_detail
            shot_columns = ['minute', 'player', 'squad', 'xg', 'psxg', 
                            'outcome', 'distance', 'bodypart', 'notes', 
                            'assist_player1', 'assist1', 'assist_player2', 'assist2']
            shot_output = html_output[-3]
            shot_output = shot_output.set_axis(shot_columns, axis=1)
            shot_output['match_id'] = match_id
            shot_output = shot_output.dropna(subset=['minute'])
            shots = pd.concat([shots, shot_output]).reset_index().drop(columns=['index'])

            # Extract goalkeeper statistics from both goalkeeper
            gk_columns = ['player', 'age', 'min', 'shots', 'goals',
            'saves', 'save_perc', 'psxg', 'launch_completion', 
            'launch_attempts', 'launch_comp_percentage', 
            'pass_attempt', 'throws', 'launch_percentage', 
            'pass_average_length', 'goalkicks', 'goalkicks_launched_percentage',
            'goalkicks_average_length', 'crosses', 'crosses_stopped', 
            'crosses_stopped_percentage', 'actions_outside_penaltyarea', 'actions_average_distance']
            # Neglect nation column from goalkeeper if existent
            gk1_output = html_output[9].drop(["('Unnamed: 1_level_0', 'Nation')"], axis=1, errors='ignore')
            gk2_output = html_output[16].drop(["('Unnamed: 1_level_0', 'Nation')"], axis=1, errors='ignore')
            gk_all_output = pd.concat([gk1_output, gk2_output])
            gk_all_output = gk_all_output.set_axis(gk_columns, axis=1)
            gk_all_output['match_id'] = match_id
            gk_stats = pd.concat([gk_stats, gk_all_output]).reset_index().drop(columns=['index'])

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
        shots['squad'] = shots['squad'].map(lambda x: x.split(' ')[1])
        shots['competition'] = competition
        shots['season'] = season

        # Add competition to goalkeeper stats
        gk_stats['competition'] = competition
        gk_stats['season'] = season

        return shots, gk_stats