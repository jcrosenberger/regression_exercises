import pandas as pd

# My env module
import src.env as env




def acquire_zillow_2017():
    '''
    This function passes a SQL query for specified columns, converts that into a pandas dataframe and then
    returns that dataframe
    '''
    sql_query = '''
    
    SELECT bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, fips, propertylandusetypeid
    FROM properties_2017
    JOIN propertylandusetype USING (propertylandusetypeid)
    WHERE propertylandusetypeid = 261
    ''' 
    
    # reads the returned data tables into a dataframe
    df = pd.read_sql(sql_query, env.codeup_db('zillow'))
    
    return df