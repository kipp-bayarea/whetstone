# whetstone

A data pipeline that pulls teacher observation data from Whetstone Education into a relational database for analysis.

## Dependencies:

- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
git clone https://github.com/kipp-bayarea/whetstone.git
```

2. Install dependencies

- Docker can be installed directly from the website at docker.com.

3. Create .env file with project secrets

```
# Whetstone Auth Credentials
CLIENT_ID=
CLIENT_SECRET=

# Database Credentials
DB=
DB_SERVER=
DB_USER=
DB_PWD=
DB_SCHEMA=

# Email Notifications
ENABLE_MAILER=1
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
```

4. Build the container

```
$ docker build -t whetstone .
```


5. Running the job

```
$ docker run --rm -it whetstone
```