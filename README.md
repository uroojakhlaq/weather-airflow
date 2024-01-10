So this is a simple airflow project that i created.
The project works by extracting weather data of the city Dehradun, Uttarakhand,India from a public api.
The data is then stored as a dictionary and then converted to pandas dataframe.
After that a new column ID is created and is_day column is transformed to boolean.
In the end the data is appeded to S3 bucket.

The whole project is orchestrated using airflow running in a VM of Ubuntu.
