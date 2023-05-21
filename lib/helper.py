import pandas as pd


def combine_dfs(df1, *argv):
    '''
    Helper function that combines a variable number of dataframes with the same columns 
    Removes duplicates
    Resets indices
    '''
    main_df = df1
    
    for dfs in argv:
        main_df = pd.concat([main_df, dfs], ignore_index=True).drop_duplicates().reset_index(drop=True)

    return main_df
    