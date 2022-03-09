import pandas as pd 
import json, requests
import math
from functools import reduce



def get_whole_data(variable = None, unit_level= 5,years = [] , newColumnName = "Data"):
    """ A function to retrive data from the GUS API. More information about the API can be found at:
    https://api.stat.gov.pl/Home/BdlApi?lang=en 

    Args:
        variable (int, optional): variable which represents a data point. Defaults to None.
        unit_level (int, optional): unit_level represents the level of validity of data. Defaults to 5.
        years (list, optional): A list of all years of validity. Defaults to [2020].
        newColumnName (str, optional): A new name for the data column. Defaults to "Data".

    Returns:
        pandas.DataFrame: returns pandas.DataFrame if successful, otherwise returns an empty list
    """
    years_str = ""
    if variable ==None:
        print("Please provide a variable which represents a data point.")
        return []
    if len(years) ==0:
        years.append(2020)
    for year in years:
        years_str +="&year="+str(year)
    
    url = "https://bdl.stat.gov.pl/api/v1/data/by-variable/"+str(variable)+"?format=json&unit-level="+str(unit_level)+years_str + "&page-size=100"
    try:
        request_data = requests.get(url).content
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        return []
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        return []
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        return []
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)
        return []
    data = json.loads(request_data)
    totalRecords = data['totalRecords']
    if totalRecords ==0:
        print ("Something went wrong, records not found!")
        return []
    listOfRecords = []
    listOfRecords.append(pd.json_normalize(data['results'], record_path = "values", meta = ["id","name"]))
    pages = math.ceil(totalRecords/100)
    page = 1
    attempts = 0 
    while page <pages:
        newUrl = url + "&page="+str(page)
        loaded = True 
        try:
            cont = requests.get(newUrl).content
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            loaded = False
            page = pages
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            loaded = False
            attempts +=1
        except requests.exceptions.RequestException as err:
            loaded = False
            print ("OOps: Something Else",err)
            attempts +=1
        if loaded == True:
            data = json.loads(cont)
            listOfRecords.append(pd.json_normalize(data['results'], record_path = "values", meta = ["id","name"]))
            page +=1
            attempts = 0
        if attempts == 10:
            page = pages 
    
    appended_data = pd.concat(listOfRecords, ignore_index = True)
    try:
        appended_data = appended_data.drop(columns = ['attrId'])
        appended_data = appended_data.rename(columns={'name': 'area', 'val': newColumnName})
        appended_data = appended_data.reindex(columns=['area','id','year', newColumnName])
    except Exception: 
        pass
    return appended_data

def merge_data(dataFrames, _on = ['id','area','year'], drop = ['id']):
    """A function to merged data from a list of dataframes. Designed to work with data retrieved from GUS API.
         https://api.stat.gov.pl/Home/BdlApi?lang=en 
    Args:
        dataFrames (list): list of pandas.DataFrame
        _on (list, optional): list of columns to merge on. Defaults to ['id','area','year'].
        drop (list, optional): list of columns to drop. Defaults to ['id'].

    Returns:
        list: If successful returns merged dataframe otherwise returns empty list
    """
    data = dataFrames
    try:
        data = reduce(lambda df1,df2: pd.merge(df1,df2,on= _on), dataFrames)
    except Exception: 
        print("Couldn't merge data frames")
        return []
    
    try:
        data=data.drop(columns = drop)
    except Exception: 
        print("Couldn't drop column/columns")
        return data
    
    return data


def retrive_multiple_data(variables = [], new_column_names = [], unit_level= 5, years = [],  _on = ['id','area','year'], drop = ['id'] ):
    """A function to retrive multiple data points from GUS API. The function merges data points into a single data frame.  

    Args:
        variables (list, optional): List of variables which represents data points. Defaults to [].
        new_column_names (list, optional):  A list of new names for the data columns. Defaults to [].
        unit_level (int, optional): unit_level represents the level of validity of data. Defaults to 5.
        years (list, optional): A list of all years of validity. Defaults to [2020].
        _on (list, optional): list of columns to merge on. Defaults to ['id','area','year'].
        drop (list, optional): list of columns to drop. Defaults to ['id'].

    Returns:
        pandas.DataFrame: returns pandas.DataFrame if successful, otherwise returns an empty list
    """
    if len(variables) != len(new_column_names):
        print("The number of column names must match the number of variables")
        return []
    dataFrames = []
    for index in range(len(variables)):
        frame = get_whole_data(variables[index], unit_level, years, new_column_names[index])
        if type(frame) is not list:
            dataFrames.append(frame)
        else:
            print("Couldn't retrive data for variable ", str(variables[index]))
    
    data = merge_data(dataFrames, _on =_on, drop = drop)
    return data
    
          
