1. Use a generator
Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

Answer the following questions:

Question 1: What is the sum of the outputs of the generator for limit = 5?
```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)
sum_value=0
for sqrt_value in generator:
    sum_value+=sqrt_value
print("sum_value",sum_value)
```
```
sum_value 8.382332347441762
```

Question 2: What is the 13th number yielded
```
# Example usage:
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)
sum_value=0
for sqrt_value in generator:
  print("sqrt_value", sqrt_value)
```
```
sqrt_value 3.605551275463989
```


2. Append a generator to a table with existing data
Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data
```
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)
```



1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
```
import dlt

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_1(),
                               table_name="people",
                               write_disposition="replace")

import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

sum_of_ages = conn.sql("SELECT sum(age) FROM people").df()
display(sum_of_ages)


```
```
Loaded tables:
sum(age)
0	140.0
```
2. Append the second generator to the same table as the first.
```
import dlt

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_2(),
                               table_name="people",
                               write_disposition="append")

```
3. After correctly appending the data, calculate the sum of all ages of people.

```
import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

sum_of_ages = conn.sql("SELECT sum(age) FROM people").df()
display(sum_of_ages)

```
```
Loaded tables:
sum(age)
0	353.0
```

Question 3. Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people
```
import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

sum_of_ages = conn.sql("SELECT sum(age) FROM people").df()
display(sum_of_ages)
```

3. Merge a generator

Re-use the generators from Exercise 2.

1. A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

2. Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

3. After loading, you should have a total of 8 records, and ID 3 should have age 33.

```
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_1(),
                               table_name="merged_people",
                               primary_key="ID",
                               write_disposition="merge")

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_2(),
                               table_name="merged_people",
                               primary_key="ID",
                               write_disposition="merge")

```
should have a total of 8 records
```
import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

count_total = conn.sql("SELECT count(*) FROM merged_people").df()
display(count_total)
```
```
Loaded tables:
count_star()
0	8

```
ID 3 should have age 33
```
import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

count_total = conn.sql("SELECT * FROM merged_people where ID=3").df()
display(count_total)

```
```
Loaded tables:
id	name	age	city	_dlt_load_id	_dlt_id	occupation
0	3	Person_3	33	City_B	1708319608.2654123	AC6etC1lREVp/g	Job_3

```


Question 4. Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above. (1 point)
```
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_1(),
                               table_name="merged_people",
                               primary_key="ID",
                               write_disposition="merge")

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_2(),
                               table_name="merged_people",
                               primary_key="ID",
                               write_disposition="merge")
```
sum of ages of all the people loaded as described abov
```
import duckdb
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables:')

count_total = conn.sql("SELECT count(*) FROM merged_people").df()
display(count_total)
```

```
Loaded tables:
sum(age)
0	266.0
```
