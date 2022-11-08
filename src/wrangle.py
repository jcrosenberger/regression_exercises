import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split


# My env module
import src.env as env


# turn off pink warning boxes
import warnings
warnings.filterwarnings("ignore")



####################################
###### SQL Query for Server ########
####################################

def sql_zillow_2017():
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
    
    # this is a very big dataset, so we handle null values by dropping them
    # df = df.isnull().sum()
    
    # Cache data
    df.to_csv('data/zillow_2017.csv')
    
    return df



##################################################
###### Initiates data pull from SQL Server #######
##################################################

def acquire_zillow_2017():
    '''
    This function reads in 2017's zillow data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a DataFrame.
    '''
    
    #checks to see if zillow data exists already
    if os.path.isfile('data/zillow_2017.csv'):
        
        df = pd.read_csv('data/zillow_2017.csv', index_col=0)
        
    else: 
        
        df = sql_zillow_2017()
    
    # this is a very big dataset, so we handle null values by dropping them
    # df = df.isnull().sum()

    # Cache data
    #    df.to_csv('data/zillow_2017_dropped_nulls.csv')
    
    df.columns = ['drop', 'bedrooms', 'bathrooms', 'sq_feet', 'tax_value', 'year_built', 'tax_amount', 'fips', 'property_type']
    return df



###################################################
###### Cleans zillow sample, handling naans #######
###################################################

def wrangled_zillow_2017():
    '''
    This is a very large dataset and the values can get very wide so we will handle null values by dropping them.
    The process will be to first turn whitespace into null values and then drop rows with null values from columns 
    which we find to be important.  
    '''


    # checks to see if wrangled zillow data exists already
    # if it does, then fills df with the stored data
    if os.path.isfile('data/wrangled_zillow_2017.csv'):

        df = pd.read_csv('data/wrangled_zillow_2017.csv')


    else:
        df = acquire_zillow_2017()

        

        # fills whitespace will Naans
        df = df.replace(r'^\s*s', np.NaN, regex=True)

        # the columns which we want to drop naan values from
        drop_columns = ['calculatedfinishedsquarefeet', 'taxamount', 'yearbuilt', 'taxvaluedollarcnt']


        # drop naans based on the columns identified above
        df = df.dropna(subset = drop_columns)

        # Cache data
        df.to_csv('data/wrangled_zillow_2017.csv')



    return df 




###########################################################################
#######      Functions for Splitting Data for Machine Learning      #######
###########################################################################

##############              First Split              ##############

def split(df):
    '''
    function to split dataframe into portions for training a model, validating the model, 
    with the goal of ultimately testing a good model
    ''' 
    
    # splits data into two groups, holding the test variable to the side
    train_validate, test = train_test_split(df, test_size = 0.2)
    
    # splits train_validate into two groups, train and validate
    train, validate = train_test_split(train_validate, test_size = 0.3)
    
    # returns train, validate, and test variables
    return train, validate, test



##############              Second Split              ##############

def x_y(df, target):
    '''
    This function depends on the split function, being handed a dataframe
    and the target variable we are seeking to understand through prediction
    '''
    
    x_train = train.drop(columns=[target])
    y_train = train[target]
    
    x_validate = validate.drop(columns=[target])
    y_validate = validate[target]
    
    x_test = test.drop(columns=[target])
    y_test = test[target]
    
    return x_train, y_train, x_validate, y_validate, x_test, y_test