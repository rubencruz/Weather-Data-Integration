# Setup guide

## 1. GitHub

Create an empty repository, for example:

`weather-data-integration`

Then:

```bash
git init
git add .
git commit -m "chore: initialize weather data integration project"
git branch -M main
git remote add origin <YOUR_GITHUB_REPOSITORY_URL>
git push -u origin main
```

Create `develop`:

```bash
git checkout -b develop
git push -u origin develop
```

## 2. PostgreSQL Docker

```bash
docker compose up -d postgres
docker compose ps
```

The local connection is:

```text
host=localhost
port=5432
database=weather
user=weather_app
password=weather_dev_password
```

Change the development password before using the container beyond a local sandbox.

## 3. Databricks

Create a Databricks Free Edition workspace. Free Edition is serverless-only and has usage quotas, so keep the project workload small.

Connect the GitHub repository through a Databricks Git folder.

For interactive development, Git folders are appropriate. For CI/CD, the repository includes Declarative Automation Bundle files.

## 4. Open-Meteo

No API key is required for the basic Open-Meteo API.

Test from your local machine:

```bash
python -c "import requests; print(requests.get('https://api.open-meteo.com/v1/forecast?latitude=-15.793889&longitude=-47.882778&current=temperature_2m').json())"
```

Then run the Databricks connectivity notebook.

## 5. PostgreSQL from Databricks

Do not use:

```text
jdbc:postgresql://localhost:5432/weather
```

inside Databricks. `localhost` would refer to the Databricks compute environment.

Use a secure, reachable endpoint instead, such as a private network connection or a controlled TCP tunnel. Store credentials in Databricks secrets or job configuration.

Example:

```text
jdbc:postgresql://reachable-host.example:5432/weather
```
