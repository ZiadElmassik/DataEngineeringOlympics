import datetime
import os
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# CHANGE THESE PATHS ACCORDING TO YOUR OWN DIRECTORY
athlete_events_path = os.path.abspath("/home/ubuntu/airflow/dags/raw_data/athlete_events.csv")
noc_regions_path = os.path.abspath("/home/ubuntu/airflow/dags/raw_data/noc_regions.csv")
summer_olympics_path = os.path.abspath("/home/ubuntu/airflow/dags/raw_data/Summer_olympic_Medals.csv")
winter_olympics_path = os.path.abspath("/home/ubuntu/airflow/dags/raw_data/Winter_Olympic_Medals.csv")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,   
    'start_date': days_ago(2),
    'email': ['ziad.elmassik@student.guc.edu.eg'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=15),
}

dag = DAG(
    'DE_MS3_dag',
    default_args=default_args,
    description="ETL pipeline for Data Engineering MS3",
    schedule_interval= "@once",
)

def extract(**kwargs):
    ti = kwargs['ti']

    df_athlete = pd.read_csv(athlete_events_path)
    df_athlete_copy = df_athlete.copy()

    df_noc = pd.read_csv(noc_regions_path)
    df_noc_copy = df_noc.copy()

    df_summer = pd.read_csv(summer_olympics_path)
    df_summer_copy = df_summer.copy()

    df_winter = pd.read_csv(winter_olympics_path)
    df_winter_copy = df_winter.copy()

    ti.xcom_push('athletes_events', (df_athlete_copy.to_json()))    
    ti.xcom_push('noc_regions', (df_noc_copy.to_json()))
    ti.xcom_push('summer_olympics', (df_summer_copy.to_json()))
    ti.xcom_push('winter_olympics', (df_winter_copy.to_json()))


def clean(**kwargs):
    ti = kwargs['ti']
    extracted_data_json = ti.xcom_pull(task_ids='extract', key='athletes_events')
    extraced_noc_regions_json = ti.xcom_pull(task_ids='extract', key='noc_regions')
    extracted_summer_json = ti.xcom_pull(task_ids='extract', key='summer_olympics')
    extracted_winter_json = ti.xcom_pull(task_ids='extract', key='winter_olympics')

    extracted_data = pd.read_json(extracted_data_json, dtype=False)
    extraced_noc_regions = pd.read_json(extraced_noc_regions_json, dtype=False)
    df_olympics_summer = pd.read_json(extracted_summer_json, dtype=False)

    #Let's start with imputing the age, given that it is essentially independent from the other variables, let's use univariate imputation to fill its missing values.
    dfAgeImputed = imputeAge(extracted_data)

    #Height and weight are heavily dependent on the sport in which the athlete is competing and the sex of the athlete, 
    #since males are typically heavier and taller than females, 
    #and certain athletes like basketball players tend to be taller and heavier than athletes in some other sports. 
    #Because of this, we will impute height and weight based on sex and sport.
    dfHeightImputed = imputeHeight(dfAgeImputed)

    #We will handle the weight in the same manner as the height, for both males and females. The same steps used above for the height are used here, for the weight.
    dfWeightImputed = imputeWeight(dfHeightImputed)

    #Assumption made: NaN in the medal value represents that the player did not win a medal, will just impute with none
    dfWeightImputed["Medal"].fillna("None", inplace= True)

    #--------------------------- Team Imputation -------------------------------

    #Noticed that teams in some records do not correspond to their respective NOC which we believe does not make
    #sense, because if a player is from a country then their team represents that country. An example
    #of an NOC not matching with the team lies in record 3, in which team is Denmark/Sweden but NOC is DEN

    #To handle this we will use the regions in NOC to fill the teams

    #Will store each NOC and its corresponding region in a dictionary, which will be referenced when filling the team values
    DictionaryNOCRegions = {}
    for index, row in extraced_noc_regions.iterrows():
        DictionaryNOCRegions.update({extraced_noc_regions.iloc[index,0]:extraced_noc_regions.iloc[index,1]})

    #Here, we use the NOC of each record to reference the dictionary and use the region to replace the old team value
    for index, row in dfWeightImputed.iterrows():
        value = str(DictionaryNOCRegions.get(row["NOC"]))
        dfWeightImputed.at[index, 'Team'] = value

    #We realised that the 'noc_regions' dataset has regions that are equal to 'nan' (not NaN) for certain NOCs such as 'ROT'. The code 'SGP' was also missing entirely from 'noc_regions' while being present in 'athlete_events'.

    #Noticed that SGP has nan for Team in athlete_events, research has granted us knowledge that SGP stands for Singapore.
    #With this knowledge, we can impute the missing team values associated with the NOC, "SGP"
    for index, row in dfWeightImputed.iterrows():
        value = row["NOC"]
        if (value == "SGP"):
            dfWeightImputed.at[index, 'Team'] = "Singapore"

    #Replacing 'nan' with NaN
    dfWeightImputed['Team'].replace('nan', np.nan, inplace=True)

    #Now that we know which Teams are equal to NaN, we can google each of those teams' NOCs and find the corresponding team value for imputation.
    for index, row in dfWeightImputed.iterrows():
        value = row["NOC"]
        if (value == "ROT"):
            dfWeightImputed.at[index, 'Team'] = "Team of Refugee Olympic Athletes"

    for index, row in dfWeightImputed.iterrows():
        value = row["NOC"]
        if (value == "TUV"):
            dfWeightImputed.at[index, 'Team'] = "Tuvalu"
        elif (value == "UNK"):
            dfWeightImputed.at[index, 'Team'] = "Unknown"


    #Models tend to work better with numerical values, let's do some encoding on the sex column given there's only 2 values, and it's categorical
    dfCleanAthlete = pd.get_dummies(dfWeightImputed, columns= ["Sex"])

    dfCleanAthlete.drop_duplicates(inplace=True)

    #Since we're only going to be adding the region column from noc_regions, let's filter out the column that we don't need. We need to keep "NOC" to merge on it, and of course the "region" column.
    regions_df = extraced_noc_regions[["NOC","region"]]

    #---------------------------------------- Clean Summer Olympics Dataset -------------------------------------------------

    DictionaryNOCRegions = {}
    for index, row in extraced_noc_regions.iterrows():
        DictionaryNOCRegions.update({extraced_noc_regions.iloc[index,0]:extraced_noc_regions.iloc[index,1]})

    #Let's use the previously constructed "DictionaryNOCRegions" constructed from the noc_regions dataframe to impute these missing NOCs. However, since the NOC value is what's missing and not the country, we will swap the keys with the values.
    DictionaryNOCRegionsWithCountryAsKey = {v:k for k, v in DictionaryNOCRegions.items()}

    #Now, we can loop on the summer olympics dataframe and impute the missing NOCs using the country name column in the Summer olympics dataframe as a key for the dictionary.
    for index, row in df_olympics_summer.iterrows():
        value = str(DictionaryNOCRegionsWithCountryAsKey.get(row["Country_Name"]))
        if (pd.isna(row["Country_Code"])):
            df_olympics_summer.at[index, 'Country_Code'] = value


    # Five more records have country codes set to 'None', we can impute them manually by knowing their values for country name.
    for index, row in df_olympics_summer.iterrows():
        if ((row["Country_Code"] == "None") & (row["Country_Name"] == "United States")):
            df_olympics_summer.at[index, 'Country_Code'] = "USA"
        if ((row["Country_Code"] == "None") & (row["Country_Name"] == "Great Britain")):
            df_olympics_summer.at[index, 'Country_Code'] = "GBR"
        if ((row["Country_Code"] == "None") & (row["Country_Name"] == "Trinidad and Tobago")):
            df_olympics_summer.at[index, 'Country_Code'] = "TTO"  
        if ((row["Country_Code"] == "None") & (row["Country_Name"] == "Chinese Taipei")):
            df_olympics_summer.at[index, 'Country_Code'] = "TPE"  
        if ((row["Country_Code"] == "None") & (row["Country_Name"] == "Independent Olympic Athletes")):
            df_olympics_summer.at[index, 'Country_Code'] = "IOA"  


    ti.xcom_push('clean_athlete_events', (dfCleanAthlete.to_json()))
    ti.xcom_push('clean_noc_regions', (regions_df.to_json()))
    ti.xcom_push('clean_summer_olympics', (df_olympics_summer.to_json()))
    ti.xcom_push('clean_winter_olympics', extracted_winter_json)

    


def imputeAge(df_athlete_copy):
    #get mean age
    mean_age= round(df_athlete_copy['Age'].mean())

    #Replace NaN age values with mean age
    df_athlete_copy["Age"].fillna(mean_age, inplace= True)

    return df_athlete_copy

def imputeHeight(df_athlete_copy):

    #----------------- To impute height, we will use multivariate imputation with Sex and Sport as our variables. -------------

    #Let's first start by obtaining the average height for male athletes in each sport.
    MeanMaleHeight = round(df_athlete_copy[df_athlete_copy["Sex"] == "M"].groupby(["Sport"])["Height"].mean())

    #Some of the means are NaN:
    #This implies the height values are ALL missing for those sports,
    #which means we won't be able to impute those values using the average of the heights in the same sport and the sex, 
    #we will use just the average for the sex to help impute these values

    #First, let's get a bit more organized and split the results of our groupby's into values that aren't NaN and values that are
    MaleHeightSportAsDictionary = MeanMaleHeight.apply(float).to_dict()

    #To do this, let's first obtain the ones with means equal to NaN, they will be given special treatment
    MaleHeightSportAsDictionaryWithNans = {}
    for key in MaleHeightSportAsDictionary.keys():
        if (pd.isna(MaleHeightSportAsDictionary.get(key))):
            MaleHeightSportAsDictionaryWithNans.update({key: MaleHeightSportAsDictionary.get(key)})

    #Now let's obtain the ones with mean equal to a value, we can use multivariate imputation according to plan
    MaleHeightSportAsDictionaryWithoutNans = {}
    for key in MaleHeightSportAsDictionary.keys():
        if ((pd.isna(MaleHeightSportAsDictionary.get(key)))==False):
            MaleHeightSportAsDictionaryWithoutNans.update({key: MaleHeightSportAsDictionary.get(key)})

    #Loop on height values that aren't NaN, then impute the NaNs for each sport where the sex is male, with the corresponding value in the dictionary.
    for k, v in MaleHeightSportAsDictionaryWithoutNans.items():
        dfForSport = df_athlete_copy[df_athlete_copy["Sex"] == "M"]
        dfForSport = dfForSport[dfForSport["Sport"] == k]
        dfForSport["Height"] = dfForSport["Height"].fillna(value=v)
        df_athlete_copy = df_athlete_copy.combine_first(dfForSport)

    #For the sports that have NaN height means, where the sex is male, we will use only the mean height for ALL males (not per sport) to impute those values.
    GlobalMeanMaleHeight = round(df_athlete_copy[df_athlete_copy["Sex"] == "M"]["Height"].mean())

    #For each sport, having a mean height of NaN, where the sex is male, use the mean height for all males to impute the missing values.
    for k, v in MaleHeightSportAsDictionaryWithNans.items():
        df_MaleHeightGlobal = df_athlete_copy[df_athlete_copy["Sex"]== "M"]
        df_MaleHeightGlobal = df_MaleHeightGlobal[df_MaleHeightGlobal["Sport"] == k]
        df_MaleHeightGlobal["Height"] = df_MaleHeightGlobal["Height"].fillna(GlobalMeanMaleHeight)
        df_athlete_copy = df_athlete_copy.combine_first(df_MaleHeightGlobal)

    #Let's do the same for the female heights that are NaN
    MeanFemaleHeight = round(df_athlete_copy[df_athlete_copy["Sex"] == "F"].groupby(["Sport"])["Height"].mean())

    #-----------Same approach for imputing missing height values for females as males--------------
    FemaleHeightSportAsDictionary =  MeanFemaleHeight.apply(float).to_dict()

    #Female Athletes will now recieve the same treatment as the males in terms of height imputation
    FemaleHeightSportAsDictionaryWithNans = {}
    for k, v in FemaleHeightSportAsDictionary.items():
        if (pd.isna(v)):
            FemaleHeightSportAsDictionaryWithNans.update({k:v})

    FemaleHeightSportAsDictionaryWithoutNans = {}   
    for k, v in FemaleHeightSportAsDictionary.items():
        if (pd.isna(v) == False):
            FemaleHeightSportAsDictionaryWithoutNans.update({k:v})

    #Same approach for mean heights that aren't equal to NaN.
    for k, v in FemaleHeightSportAsDictionaryWithoutNans.items():
        dfFemaleHeight = df_athlete_copy[df_athlete_copy["Sex"] == "F"]
        dfFemaleHeight = dfFemaleHeight[dfFemaleHeight["Sport"] == k]
        dfFemaleHeight["Height"] = dfFemaleHeight["Height"].fillna(value=v)
        df_athlete_copy = df_athlete_copy.combine_first(dfFemaleHeight)

    #Mean female height for all sports will be used to impute height for sport with NaN means for females, just as with males.
    GlobalMeanFemaleHeight = round(df_athlete_copy[df_athlete_copy["Sex"] == "F"]["Height"].mean())

    for k, v in FemaleHeightSportAsDictionaryWithNans.items():
        dfFemaleHeight = df_athlete_copy[df_athlete_copy["Sex"] == "F"]
        dfFemaleHeight = dfFemaleHeight[dfFemaleHeight["Sport"] == k]
        dfFemaleHeight["Height"] = dfFemaleHeight["Height"].fillna(GlobalMeanFemaleHeight)
        df_athlete_copy = df_athlete_copy.combine_first(dfFemaleHeight)


    return df_athlete_copy


def imputeWeight(df_athlete_copy):

    #The exact same logic as imputeHeight was used here.
    #Let's first start by obtaining the average weight for male athletes in each sport.
    WeightSportMale = round(df_athlete_copy[df_athlete_copy["Sex"] == "M"].groupby(["Sport"])["Weight"].mean())
    
    WeightSportMaleDictionary = WeightSportMale.apply(float).to_dict()

    WeightSportMaleDictionaryWithNans = {}
    for k, v in WeightSportMaleDictionary.items():
        if (pd.isna(v)):
            WeightSportMaleDictionaryWithNans.update({k:v})
    
    WeightSportMaleDictionaryWithoutNans = {}
    for k, v in WeightSportMaleDictionary.items():
        if (pd.isna(v) == False):
            WeightSportMaleDictionaryWithoutNans.update({k:v})

    for k, v in WeightSportMaleDictionaryWithoutNans.items():
        dfForSport = df_athlete_copy[df_athlete_copy["Sex"] == "M"]
        dfForSport = dfForSport[dfForSport["Sport"] == k]
        dfForSport["Weight"] = dfForSport["Weight"].fillna(value=v)
        df_athlete_copy = df_athlete_copy.combine_first(dfForSport)
        
    GlobalMaleWeight = round(df_athlete_copy[df_athlete_copy["Sex"] == 'M']["Weight"].mean())

    for k, v in WeightSportMaleDictionaryWithNans.items():
        dfForSport = df_athlete_copy[df_athlete_copy["Sex"] == "M"]
        dfForSport = dfForSport[dfForSport["Sport"] == k]
        dfForSport["Weight"] = dfForSport["Weight"].fillna(value=GlobalMaleWeight)
        df_athlete_copy = df_athlete_copy.combine_first(dfForSport)

    WeightSportFemale = round(df_athlete_copy[df_athlete_copy["Sex"] == "F"].groupby(["Sport"])["Weight"].mean())

    WeightSportFemaleDictionary = WeightSportFemale.apply(float).to_dict()

    WeightSportFemaleDictionaryWithoutNans = {}
    WeightSportFemaleDictionaryWithNans = {}

    for k, v  in WeightSportFemaleDictionary.items():
        if (pd.isna(v) == False):
            WeightSportFemaleDictionaryWithoutNans.update({k:v})
        else:
            WeightSportFemaleDictionaryWithNans.update({k:v})

    for k, v in WeightSportFemaleDictionaryWithoutNans.items():
        dfForSport = df_athlete_copy[df_athlete_copy["Sex"] == "F"]
        dfForSport = dfForSport[dfForSport["Sport"] == k]
        dfForSport["Weight"] = dfForSport["Weight"].fillna(value=v)
        df_athlete_copy = df_athlete_copy.combine_first(dfForSport)

    GlobalFemaleWeight = round(df_athlete_copy[df_athlete_copy["Sex"] == "F"]["Weight"].mean())

    for k,v in WeightSportFemaleDictionaryWithNans.items():
        dfForSport = df_athlete_copy[df_athlete_copy["Sex"] == "F"]
        dfForSport = dfForSport[dfForSport["Sport"] == k]
        dfForSport["Weight"] = dfForSport["Weight"].fillna(value=GlobalFemaleWeight)
        df_athlete_copy = df_athlete_copy.combine_first(dfForSport)

    return df_athlete_copy


def integrate(**kwargs):
    ti = kwargs['ti']
    df_athlete_copy_json = ti.xcom_pull(task_ids='clean', key='clean_athlete_events')
    regions_df_json = ti.xcom_pull(task_ids='clean', key='clean_noc_regions')
    df_olympics_summer_json = ti.xcom_pull(task_ids='clean', key='clean_summer_olympics')
    df_olympics_winter_json = ti.xcom_pull(task_ids='clean', key='clean_winter_olympics')

    df_athlete_copy = pd.read_json(df_athlete_copy_json, dtype=False)
    regions_df = pd.read_json(regions_df_json, dtype=False)
    df_olympics_summer = pd.read_json(df_olympics_summer_json, dtype=False)
    df_olympics_winter = pd.read_json(df_olympics_winter_json, dtype=False)

    #----------------------------- Integration 1 ---------------------------

    #Since the NOC columns from the newly cleaned "athlete_events", and noc_regions may have different values, let's use "NOC" as the key for a left join. This will preserve all columns in the left dataset. 
    #We'll handle conflicts in key values later on.
    df_merged = pd.merge(df_athlete_copy, regions_df, on="NOC", how="left")
    df_merged.rename(columns={"region":"Region"}, inplace=True)

    #Found 4 Regions having NaN values, let's add them manually
    for index, row in df_merged.iterrows():
        value = row["NOC"]
        if (value == "TUV"):
            df_merged.at[index,'Region'] = "Tuvalu"
        elif (value == "UNK"):
            df_merged.at[index,'Region'] = "Unknown"
        elif (value == "ROT"):
            df_merged.at[index,'Region'] = "Team of Refugee Olympic Athletes/Undefined"
        elif (value == "SGP"):
            df_merged.at[index,'Region'] = "Singapore"

 
    #---------------------------- Integration 2 ---------------------------------------
    #Since the Summer and Winter dataframes have the same columns and different rows, we can just append the two dataframes, to create one dataframe that we can integrate into our main dataset.
    df_medals = df_olympics_summer.append(df_olympics_winter)
    df_medals = df_medals.sort_values(by=['Year'])
    

    #Since, we will use NOC as adf_medals key for integration, let's rename the country code column in the dataframe resulting from appending summer and winter (df_medals), to 'NOC' to ensure that both datasets use the same name for the key.
    df_medals.rename(columns={"Country_Code":"NOC"}, inplace=True)

    #Dropping duplicate rows from the df_medals dataframe could be useful in eliminating any redundant NOC keys. Let's also drop the columns that we don't need.
    df_medals.drop_duplicates(["NOC","Year"],inplace=True)
    df_medals.drop(["Host_country"],axis=1,inplace=True)
    df_medals.drop(["Host_city"],axis=1,inplace=True)
    df_medals.drop(["Country_Name"],axis=1,inplace=True)

    #For the merge, an inner join will be performed between the dataset produced from appending the summer and winter olympics datasets, and the athlete events dataset.athlete_events_path
    #The columns, "NOC", and "Year" will be used as the keys for the join.
    df_merged = pd.merge(df_merged,df_medals,on=["NOC","Year"],how="inner")

    #We will also rename the new columns, giving them more descriptive names.
    df_merged.rename(columns={"Gold":"Team_Gold_Per_Year"},inplace=True)
    df_merged.rename(columns={"Silver":"Team_Silver_Per_Year"},inplace=True)
    df_merged.rename(columns={"Bronze":"Team_Bronze_Per_Year"},inplace=True)

    ti.xcom_push('integrated_data', (df_merged.to_json()))


def feature_engineer(**kwargs):

    ti = kwargs['ti']
    df_merged_json = ti.xcom_pull(task_ids='integrate', key='integrated_data')
    df_merged = pd.read_json(df_merged_json, dtype=False)

    #Now that we have integrated the number of bronze, silver, and gold medals won per year by each team, into our dataset, we can engineer the feature describing the total number of medals won by the team per year.
    df_merged["Total_Medals_Per_Year"] = df_merged["Team_Gold_Per_Year"] + df_merged["Team_Silver_Per_Year"] + df_merged["Team_Bronze_Per_Year"]

    #For the second feature, it would be quite useful to determine the body mass index (BMI) of each individual athlete.
    df_merged['BMI']=((df_merged['Weight'])/(df_merged['Height']**2)*10000)

    ti.xcom_push('featureDone', (df_merged.to_json()))


def load(**kwargs):

    ti = kwargs['ti']
    complete_data_json = ti.xcom_pull(task_ids='feature_engineering', key='featureDone')

    complete_data = pd.read_json(complete_data_json, dtype=False)

    #CHANGE THIS PATH ACCORDING TO YOUR OWN DIRECTORY
    complete_data.to_csv('/home/ubuntu/airflow/dags/athlete_events_transformed.csv', index=False)


extract_task = PythonOperator(
    task_id = "extract",
    python_callable=extract,
    dag=dag
)

clean_task = PythonOperator(
    task_id = "clean",
    python_callable=clean,
    dag=dag
)

integrate_task = PythonOperator(
    task_id = "integrate",
    python_callable=integrate,
    dag=dag
)

feature_engineering_task = PythonOperator(
    task_id = "feature_engineering",
    python_callable=feature_engineer,
    dag=dag
)

load_task = PythonOperator(
    task_id = "load",
    python_callable=load,
    dag=dag
)

extract_task >> clean_task >> integrate_task >> feature_engineering_task >> load_task