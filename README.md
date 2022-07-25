#Overview of the dataset:

This dataset is comprised of records of athletes in the olympics since the start of the olympics. Their attributes include: Name of athlete, their sex, their age, their height, their weight, the team they represent, the sport in which they compete, and what medal they earned, if any. It also includes the country code (NOC), the year in which they competed, the city in which they competed, the season, and the year.

#Goals and motivation:

The goal was to gain insight from the olympic records, regarding the performance of the countries, the performance of the athletes with respect to their attributes, as well as the sports. We are searching for observable patterns or relations between attributes that might be useful in the future planning of the olympics.

#Steps used for the work done in this milestone:

Started with the age attribute, where missing values were imputed using only the average of the age of athletes (univariate impuation), given that the age column was normally distributed and independent from other attributes.

A boxplot was plotted to visualise the distribution of the age column, before and after imputation. We observed that age was normally distributed, hence we used mean for imputation.

Next, we handled missing values in the height and weight columns in order to yield accurate results, we performed multi-variate impuation, in which the independent variables were the sex, and the sport. This was done as height and weight would vary depending on both the sport (Gymnastics and Rugby, for example), and on the biological differences between males and females.

Outliers, were maintained for all numerical columns, as they were regarded as being interesting (for example, an athlete competing at the age of 90, or an athlete starting off at a very young age, or one being abnormally tall). Making alterations to such occurences may impact the validity of the dataset.

Afterwards, we found inconsistencies between the team names and the NOC. In most cases for example, the NOC, "DEN" is accompanied with the team name "Denmark". In one case, for example, the NOC was "DEN" but the team was "Denmark/Sweden". We made sure to resolve such inconsistencies by using the values from the region column in the 'noc_regions' dataset to replace the team values in 'athlete_events'. To replace the values, we compared the country code ('NOC') entry in 'athlete_events' with the country codes in the dataset 'noc_regions' to obtain the name of the country using the country code from 'noc_regions' and substituting the team value in 'athlete_events' with the corresponding region value from 'noc_regions'. This way if a team is present, then it is present with its corresponding NOC. We later discovered missing values in the team column taking the value "None", as well as team values being now equal to "nan" from NaN. The latter being a result of the previous replacement process of the team names. These were replaced with NaN. Then, we used the dataset, "noc_regions" in order to impute these missing values. However, not all the country codes were covered in the dataset 'noc_regions', an example would be 'SGP' which was present in 'athlete_events' but not in 'noc_regions'. There were also missing region names from the NOC dataset. For the regions and country codes that were missing from the dataset, we googled the country codes in the 'athlete_events' dataset that had missing team values and found the country name manually for the remaining countries, we then used this name to impute in the 'Team' column. The logic behind imputing using the country name into the 'Team' column was the trivial fact that in the olympics, athletes compete in the name of their country. One special case was observed at the country code 'UNK', which was later noticed that it stands for 'Unknown'. After researching the athletes and not finding a lot of their nationalities, we decided to impute the value 'Unknown' into the column 'Team', where this row contains 'UNK' in the column 'NOC'.

Finally. we visualized the columns to observe the distribution using boxplots and distplots. This was done for numerical data columns including age, height, and weight. These columns were also visualized using a pairplot to visually identify patterns and sequences through the different types of graphs and over all the data columns collectively, we also plotted a heatmap. We visualized the weight and the height distribution individually in order to compare them to the average weight and height with respect to sport as well as sex to observe the differences between them.

#Data Exploration Questions:

The research questions that are aimed to be answered through the exploration of the dataset are as follows:

1- For the sport with the most participants, which country has the most medals?

2- Which sport has the athletes having the highest average BMI (Feature Question)?

3- For the sport with the most participants, which olympian has won the most medals?

4- Which country earned the most medals in 2016 (Feature Question)?

5- Which country has the highest number of male participants?

#Description of the new added dataset:

Two external datasets were used from Kaggle. One recording the number of gold, silver, and bronze medals for every team/country in every year for the Summer Olympic Games, and one recording the same but for the Winter Olympic Games. The two datasets have the same columns and were appended onto one another to form one complete dataset to be used for integration (We'll call it df_medals). Only the necessary columns were kept in this dataset, all the others were dropped. The remaining columns include the name of the country that participated, along with its NOC, as well as the year in which it participated. Also, the number of gold, silver, and bronze medals won by the country in that year. The columns "NOC", and "Year" were used performing an inner join between the main dataset and the df_medals. The added columns were three containing the numbers of bronze, silver, and gold medals (each in one column) won by the Team in the given year. A column containing the region of the participating Team was added from the noc_regions.csv dataset using "NOC" as the key for performing a left join.

#Description of the features we added:

Two features were engineered. The first being the total number of medals won by a team in a specific year, deduced by adding the number of bronze, silver and gold medals won by the team in that year, together. The second was the body mass index (BMI) of each athlete, computed using the standard formula: (Weight/(Height^2))*10,000.

#ETL Pipeline Description:

The pipeline consists of 3 stages: extract, transform, and load; with the sequence being extract, followed by transform, and finally load.

The stages take the form of python operators/tasks in the dag. Each python operator performs the desired operation via callable functions.

In the extract stage, 4 datasets are read as csv files from their respective paths. The datasets consist of: athlete_events (the main dataset on which operations occur), noc_regions (a dataset used for resolving inconsistencies in the main dataset, as well as in integration), summer_olympics and winter_olympics (both datasets used in integration). These 4 datasets are forwarded to the transform stage.

IMPORTANT NOTE: Modify the paths in lines 10-13 to suit the tester, as the csv files are read from these paths.

The transform stage is segregated into 4 stages: clean, integrate, and feature engineering; with the sequence of actions being clean, then integrate, then feature engineering.

In the clean stage, the order of actions is as follows: 1-Missing values in the age column in the main dataset are imputed using univariate imputation, in which the mean of the ages is used. 2-Missing values in height and weight are imputed using multivariate imputation, in which the mean of the height and weight per sex and sport is used for imputing missing values in their respective columns. 3-NaN values in the medals column are replaced with the string 'None' based on the assumption that missing values in the medals column aren't actually missing but represent a lack of victory (no medals were earned). 4-noc_regions is used to resolve inconsistencies in the names of teams in the main dataset. 5-Any missing team names were imputed through external sources such as wikipedia.com and olympics.com. 6-One-hot encoding of the sex column is then, performed. 7-Duplicates in the dataset are then dropped. 8-Unnecessary columns in noc_regions are dropped. 9-In the summer_olympics dataset, there were missing values in the column, 'Country_Code'. These were imputed using the values in the column, 'NOC' in the noc_regions dataset by matching the country names from the two datasets. Any remaining values (still missing) were imputed manually. 10-This would conclude the clean stage. Finally, the 4 datasets (after the alterations mentioned previously, if any), are forwarded to the integrate stage.

In the integrate stage: 1-The new noc_regions dataset is merged with the new athlete_events dataset using a left join (athlete_events is the left dataset). 2-The column ,'region' is renamed to 'Region' just to match the naming convention. 3-After the join, some values in 'Region' were missing, they were imputed manually. 4-The summer_olympics and winter_olympics dataset were then appended to form one large dataset having all the years and countries that have ever participated in those years. 5-All unnecessary columns in this new dataset were dropped, as well as duplicates. 6-The new athlete_events dataset was then merged with the previously created dataset using an inner join. 7-The added columns were renamed to have more descriptive names. 8-Finally the athlete_events dataset after integration, was then forwarded to the feature engineering stage.

In the feature engineering stage, 2 columns were engineered: 1-The first being, 'Total_Medals_Per_Year' being computed by summing the values in the three columns obtained from integrating the athlete_events dataset with the dataset created in step 4 in the integrate stage. 2-The second being, the body mass index of each player, computed from height and width using the standard formula for calculating BMI. 3-Finally, we forward the new athlete_events having the new features to the load stage.

In the load stage: We simply download the resultant dataframe as a csv file.

IMPORTANT NOTE: Modify the path of the download to suit the tester.
