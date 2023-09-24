from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.endpoints import playerawards
from nba_api.stats.static import players
import pandas as pd
import numpy as np

#create dataframe to get all players
df = pd.DataFrame(players.get_players(), columns=['id', 'full_name','first_name','last_name',"is_active"])
#get ids to to loop thorugh and get player stats 
player_id_list = df['id']
player_name_list = df['full_name']
name = player_name_list.tolist()
p_id = player_id_list.tolist()

#get all player stats data

all_player_list = []
for i, val in enumerate(p_id):
    career = playercareerstats.PlayerCareerStats(player_id=val) 
    career = career.get_data_frames()[0]
    career['name'] = name[i]
    all_player_list.append(career)
    print(val)
    print(i)
    
all_player_list = pd.concat(all_player_list)
all_player_list.to_csv("all.csv")


all_players = pd.read_csv('all.csv')

#create df with columns we need to calculate total stats for
final_df = pd.DataFrame(columns=['name', 'PTS', 'FG_PCT', 'GP',	'MIN', 'FGM', 'REB', 'AST', 'STL','BLK'])
final_df['name'] = all_players['name']
final_df['PTS'] = all_players['PTS']
final_df['FG_PCT'] = all_players['FG_PCT']
final_df['GP'] = all_players['GP']
final_df['MIN'] = all_players['MIN']
final_df['FGM'] = all_players['FGM']
final_df['REB'] = all_players['REB']
final_df['AST'] = all_players['AST']
final_df['STL'] = all_players['STL']
final_df['BLK'] = all_players['BLK']

#calculate aggregate statistics
final_df['total_points'] = final_df.groupby('name', sort=False)['PTS'].transform('sum')
final_df['career FG_PCT'] = final_df.groupby('name', sort=False)['FG_PCT'].transform('mean')
final_df['total gp'] = final_df.groupby('name', sort=False)['GP'].transform('sum')
final_df['total min'] = final_df.groupby('name', sort=False)['MIN'].transform('sum')
final_df['total fgm'] = final_df.groupby('name', sort=False)['FGM'].transform('sum')
final_df['total reb'] = final_df.groupby('name', sort=False)['REB'].transform('sum')
final_df['total asts'] = final_df.groupby('name', sort=False)['AST'].transform('sum')
final_df['steals'] = final_df.groupby('name', sort=False)['STL'].transform('sum')
final_df['blocks'] = final_df.groupby('name', sort=False)['BLK'].transform('sum')


#remove columns we no longer need as we only want aggregate stats
transformed_final_df = final_df
transformed_final_df.drop(transformed_final_df.columns[[1,2,3,4,5,6,7,8,9]], axis=1, inplace=True)
transformed_final_df.drop_duplicates(inplace=True)
transformed_final_df.to_csv('transformed_final.csv')



#get number of all nba awards by player and join back to this table
all_player_list = []
for i, val in enumerate(p_id):
    awards = playerawards.PlayerAwards(player_id=val) 
    awards = awards.get_data_frames()[0]
    awards['name'] = name[i]
    all_player_list.append(awards)
    print(val)
    print(i)
    
all_player_list = pd.concat(all_player_list)
all_player_list.to_csv("all_awards.csv")

all_awards = pd.read_csv('all_awards.csv')

#get all nba awards df in right format
all_nba_df =  pd.DataFrame(columns=['name', 'all_nba_awards'])
all_nba_df['name'] = all_awards['name']
all_nba_df['all_nba_awards'] =np.where(all_awards['DESCRIPTION']=='All-NBA', "All-NBA","")
rslt_df = all_nba_df[all_nba_df['all_nba_awards'] !=""]
rslt_df['count'] = rslt_df.groupby('name')['name'].transform('count')
final_all_nba_awards = pd.DataFrame(columns=['name', 'all_nba_awards'])
final_all_nba_awards['name'] = rslt_df['name']
final_all_nba_awards['all_nba_awards'] = rslt_df['count']
final_all_nba_awards.drop_duplicates(inplace=True)

#merge player stats df with all nba awards df
merged_df = pd.merge(transformed_final_df,final_all_nba_awards)
merged_df.to_csv("merged.csv")