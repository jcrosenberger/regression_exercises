import pandas as pd
import os

# My env module
import src.env as env


# turn off pink warning boxes
import warnings
warnings.filterwarnings("ignore")



def sql_zillow_2017():
    '''
    This function passes a SQL query for specified columns, converts that into a pandas dataframe and then
    returns that dataframe
    '''
    sql_query = '''
    
    SELECT bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, fips, propertylandusetypeid
    FROM properties_2017
    # JOIN propertylandusetype USING (propertylandusetypeid)
    WHERE propertylandusetypeid = 261
    ''' 
    
    # reads the returned data tables into a dataframe
    df = pd.read_sql(sql_query, env.codeup_db('zillow'))
    
    # this is a very big dataset, so we handle null values by dropping them
    df = df.isnull().sum()
    
    
    return df



def acquire_zillow_2017():
    '''
    This function reads in 2017's zillow data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a DataFrame.
    '''
    
    #checks to see if zillow data exists already
    if os.path.isfile('data/zillow_2017.csv'):
        
        df = pd.read_csv('data/zillow_2017.csv', index_col=0)
        
    else: 
        
        df = sql_zillow_2017():
    
    # Cache data
        df.to_csv('data/zillow_2017.csv')
    
    
    return df