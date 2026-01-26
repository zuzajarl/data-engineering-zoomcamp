# Question 1. Understanding Docker images

docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13

pip -V 

solution: pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

# Question 2
db:5432 - db ist the hostname, 5432 is the port 
      

# Question 3

uv init --python=3.13
uv add pandas pyarrow

docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18


  uv add --dev pgcli
  uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
  We enter the password (root)

  uv add --dev jupyter
  uv run jupyter notebook


  # Question 4,5,6 available in task3.ipynb

  # Question 7

  Modified the .tf files and applied commands:


gcloud auth application-default login

terraform init

terraform plan -var="project=<your-gcp-project-id>"

terraform apply -var="project=<your-gcp-project-id>"