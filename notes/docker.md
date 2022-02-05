## Advantages of Docker
* Reproducibility
* Run tests locally
* Isolation of environments (Can also be a con at times)

## Notes
When creating your working folder, the name should be in lowercase


## Working with docker in WSL Backend
* First you will need to install docker on Windows - https://www.docker.com/products/docker-desktop
* Configure docker desktop to work with wsl - https://docs.docker.com/desktop/windows/wsl/
    * Found out it works with wsl 2. Something to think about
* When running commands, docker was having network issue. Fixed it by selecting to configure DNS manually
https://stackoverflow.com/questions/49387263/docker-error-response-from-daemon-get-https-registry-1-docker-io-v2-servic
* when mounting volume to docker, make sure you provide the linux home path. The mounted path will not work


## Some commands
* docker build -t <imagename:tag> .
* docker run -it <imagename:tag>
* To add arguments to be passed to the entrypoint file
    * docker run -it <imagename:tag> <arg1> <arg2>
* To add environment variables
    * docker run -it -e <vari_name>="<value>" <imagename:tag> 
* To mount folder so that when docker is run again you dont loose dat
    * docker run -it -v <path_to_local_folder>:<path_to_folder_in_docker> <imagename:tag>


## Training commands
start docker with postgresql:
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

  connect pgcli to postgres
  pgcli -h localhost -p 5432 -u root -d ny_taxi

start docker with pgadmin
docker run -it \
-e PGADMIN_DEFAULT_EMAIL='admin@admin.com' \
-e PGADMIN_DEFAULT_PASSWORD='root' \
-p 8080:80 \
dpage/pgadmin4

postgres and pgadmin cannot communicate since there are in different containers. There is need to connect the two. This is done through networks.
docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgress_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name=pg-database3 \
  postgres:13

  docker run -it \
-e PGADMIN_DEFAULT_EMAIL='admin@admin.com' \
-e PGADMIN_DEFAULT_PASSWORD='root' \
-p 8080:80 \
--network=pg-network \
  --name=pgadmin4 \
dpage/pgadmin4

URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}

docker build -t taxi_ingest:v001 .

URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
docker run -it \
--network=pg-network \
taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database3 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}

## PGCLI installation issues on ubuntu (if not done well, you will have issues viewing the table description)
sudo apt-get install libpq-dev python-dev
pip install pgcli

## SQLALCHEMY
if you have an issue with 'psycopg2', use pip install psycopg2-binary

## TERRAFORM
Used for managing the infrastrure with code
https://www.terraform.io/downloads
For WSL, install the ubuntu version 

## GCP
Create an account and activate the credits
Create a new project
Create a service account - This will be the account used by services such as APIs, Pipelines....
Create keys
Install gcloud sdk - https://cloud.google.com/sdk/docs/install
if its a new installation, do gcloud init before following the instructions
