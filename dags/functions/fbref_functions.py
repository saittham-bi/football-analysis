import pandas as pd

def clean_headers(df):
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    return df

def create_url(df):
    df['url'] = [x[1] for x in df['url'].tolist()]
    df.dropna(axis=0, how='all', inplace=True)
    df['url'] = 'https://fbref.com' + df['url']
    return df

def get_latest_season(url):
    # Read competition Table
    competition = pd.read_html(url)[0]

    # Extract the URL of the seasons
    url = pd.read_html(url, extract_links='all')[0].iloc[:, 0].rename('url')
    comp = pd.concat([competition, url], axis=1)
    comp = create_url(comp)

    # Rename columns
    comp = clean_headers(comp)

    current_season_url = comp['url'][0]

    return current_season_url

def get_fixtures(url, comp):
    season_url = get_latest_season(url)

    fixture_url = season_url.rsplit('/', 1)[0] + '/schedule/'
    fixtures = pd.read_html(fixture_url)[0]
    fixtures = fixtures.iloc[:, :9]
    urls = pd.read_html(fixture_url, extract_links='all')[0].iloc[:, -2].rename('url')
    fixtures = pd.concat([fixtures, urls], axis=1)
    fixtures = fixtures.dropna(subset=['Score']).copy()

    # Rename column headers
    fixtures = clean_headers(fixtures)

    # Add column
    fixtures['competition'] = comp

    # Extract URL to matches
    fixtures = create_url(fixtures)

    return fixtures

def transform_scores(df):
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
    scores = scores[['url', 'competition', 'wk', 'day', 'date', 'time',  'home', 'away', 'score_home', 'score_away', 'xg', 'xg.1']].copy()
    scores_away = scores[['url', 'competition', 'wk', 'day', 'date', 'time',  'away', 'home', 'score_away', 'score_home', 'xg.1', 'xg']].copy()

    scores.rename(columns={'home': 'team', 'away': 'opponent', 'score_home': 'score', 
                        'score_away': 'score_opp', 'xg.1': 'xg_opp'}, inplace=True)

    scores_away.rename(columns={'home': 'opponent', 'away': 'team', 'score_home': 'score_opp', 
                                'score_away': 'score', 'xg.1': 'xg', 'xg': 'xg_opp'}, inplace=True)

    # Add home venue for all games in df
    scores['venue'] = 'home'
    scores_away['venue'] = 'away'

    scores = pd.concat([scores, scores_away]).reset_index()

    scores['date'] = pd.to_datetime(scores['date'])
    scores['wk'] = scores['wk'].astype(int)

    return scores

def clean_shot_table(df):
    shots = df.dropna(subset=[df.columns[0]]).reset_index().iloc[:, 1:-2]

    headers = [i[1] for i in shots.columns]

    # Clean Header names
    shots.columns = headers
    shots.columns.values[-1] = 'Assist ' + shots.columns[-1]
    shots.columns.values[-2] = 'Assist ' + shots.columns[-2]
    
    return shots