## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

```console
SELECT COUNT(*) FROM green_tripdata WHERE lpep_pickup_datetime >= '2019-09-18 00:00:00'
   AND lpep_pickup_datetime < '2019-09-19 00:00:00' and lpep_dropoff_datetime >= '2019-09-18 00:00:00' and lpep_dropoff_datetime < '2019-09-19 00:00:00';
```

- 15612

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance. 
```console
root@localhost:ny_taxi> SELECT 
     DATE(lpep_pickup_datetime) AS pickup_day,
     SUM(trip_distance) AS total_distance
 FROM 
     green_tripdata
 GROUP BY 
     pickup_day
 ORDER BY 
     total_distance DESC
 LIMIT 1;
+------------+------------------+
| pickup_day | total_distance   |
|------------+------------------|
| 2019-09-26 | 58759.9400000002 |

- 2019-09-26
```


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 ```console
  select
     z."Borough",
     sum(t.total_amount) total_amount
 from green_tripdata t
 join taxi_zone_lookup z
 on t."PULocationID" = z."LocationID"
 where t.lpep_pickup_datetime >= '2019-09-18' and t.lpep_pickup_datetime < '2019-09-19'
 group by z."Borough"
 having sum(t.total_amount) > 50000
 order by total_amount desc;
+-----------+-------------------+
| Borough   | total_amount      |
|-----------+-------------------|
| Brooklyn  | 96333.23999999916 |
| Manhattan | 92271.2999999985  |
| Queens    | 78671.70999999919 |
+-----------+-------------------+
SELECT 3
Time: 0.094s

- "Brooklyn" "Manhattan" "Queens"
```

## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`
```console
 SELECT 
     z_dropoff."Zone" ,
    t.tip_amount AS Tip
 FROM 
     green_tripdata t
 JOIN 
     taxi_zone_lookup z_dropoff ON t."DOLocationID" = z_dropoff."LocationID"
 JOIN 
     taxi_zone_lookup z_pickup ON t."PULocationID" = z_pickup."LocationID"
 WHERE 
     z_pickup."Zone" = 'Astoria' AND 
     DATE(t.lpep_pickup_datetime) >= '2019-09-01' AND 
     DATE(t.lpep_pickup_datetime) < '2019-10-01'
 ORDER BY 
     Tip DESC
 LIMIT 1;
+-------------+-------+
| Zone        | tip   |
|-------------+-------|
| JFK Airport | 62.31 |
+-------------+-------+

- JFK Airport

```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.
```console
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/**]
google_storage_bucket.bucket: Creation complete after 1s [id=**]
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET
