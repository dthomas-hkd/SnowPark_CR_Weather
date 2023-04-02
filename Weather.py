from snowflake.snowpark import Session
from utils import snowpark_utils
import pandas as pd , urllib.request, json, sys, os


def obtain_CR_prov_ids () -> list:

    with open('city_list.json') as file:
        data = json.load(file)

    df = pd.DataFrame(data)
    
    CR_df = df[df["country"] == "CR"]

    print("\n\nAvailable CR cities")
    print(CR_df)

    CR_prov_df = CR_df[CR_df["name"].str.contains("Provincia de")]

    print("\n\nCR provinces")

    print(CR_prov_df)

    CR_prov_df['id'] = CR_prov_df['id'].astype(int)

    return CR_prov_df

def obtain_city_data(CR_prov_df):

    data = []   
    for index, row in CR_prov_df.iterrows():
        
        city_name = row['name']

        city_id =  row['id']

        print(f"\n\nEvaluating {city_name}, id {city_id}")

        # Get Weather
        response_current = urllib.request.urlopen(f'http://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}&units=metric')

        data_weather_current = json.load(response_current)

        print ("\nCurrent Weather : " + str(data_weather_current['main']['temp']) + "C")

        json_object = json.dumps(data_weather_current)

        print(json_object)

        data.append(json_object)

    data_df = pd.DataFrame({'data': data})
    
    return(data_df)


def upload_raw_data (data_df):

    session = snowpark_utils.get_snowpark_session()
       
    session.write_pandas(data_df, "raw_weather_2", quote_identifiers= False, auto_create_table=True, table_type = "temp")

    cleaned_raw_data = session.sql("select parse_json(data) as data from raw_weather_2")
    
    cleaned_raw_data.write.mode("append").save_as_table("cleaned_raw_weather")

    upload_results = session.table("cleaned_raw_weather").collect()

    print()
    print("cleaned_raw_weather current content:")
    for r in upload_results:
        print()
        print(r)

if __name__ == "__main__":

    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)
  
    api_key= os.environ["API_KEY"]  

    CR_prov_df = obtain_CR_prov_ids()

    data_df = obtain_city_data(CR_prov_df)

    upload_raw_data(data_df)