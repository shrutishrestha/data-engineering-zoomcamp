## Week 2 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

> In case you don't get one option exactly, select the closest one 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

You may need to reference the link below to download via Python in Mage:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

#### The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`

- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
 
##### answer:
```
@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    main_df = pd.DataFrame()
    url_path = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
    months=["10","11","12"]

    for month_idx in (months):
        file_name = "green_tripdata_2020-"+month_idx+".csv.gz"
        url = url_path+file_name

        print("url", url)
        taxi_dtypes = {
            'VendorID':pd.Int64Dtype(),
            'passenger_count':pd.Int64Dtype(),
            'trip_distance':float,
            'RatecodeID':pd.Int64Dtype(),
            'store_and_fwd_flag':str,
            'PULocationID':pd.Int64Dtype(),
            'DOLocationID':pd.Int64Dtype(),
            'payment_type':pd.Int64Dtype(),
            'fare_amount':float,
            'extra':float,
            'mta_tax':float,
            'tip_amount':float,
            'tolls_amount':float,
            'improvement_surcharge':float,
            'total_amount':float,
            'congestion_surcharge':float
        }

        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        sub_df = pd.read_csv(url, sep=",", compression="gzip",dtype=taxi_dtypes, parse_dates=parse_dates)
        main_df = pd.concat([main_df,sub_df], axis=0)
    print(main_df.shape)
    return main_df
```
    
#### Add a transformer block and perform the following:
##### Remove rows where the passenger count is equal to 0 _and_ the trip distance is equal to zero.
```
data = data[(data['passenger_count']!=0) & (data['trip_distance']!=0)]
```

##### Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
```
pd.read_csv(url, sep=",", compression="gzip",dtype=taxi_dtypes, parse_dates=parse_dates)
```

##### Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
```
def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
data.columns = [camel_to_snake(col) for col in data.columns]
```

##### Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
```
@test
def test_vendor_id(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'vendor_id_not in columns'
    
@test
def test_passenger_count(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passsenger'

@test
def test_trip_distance(output, *args) -> None:
    assert (output['trip_distance'] > 0).all(), 'There are tripdistance which are 0 or less then 0'
```

##### Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
```
@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
```


- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns - answer
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows - answer
* 266,856 rows

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date` - answer
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2 - answer
* 1, 2, 3, 4
* 1

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4 - answer

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96 - answer
* 56
* 67
* 108

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2
* Check the link above to see the due date
  
## Solution

Will be added after the due date
