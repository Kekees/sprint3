FROM python:3.8

WORKDIR /opt/my_etl

# Скопируйте в контейнер файлы, которые редко меняются
COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt


COPY /psql_to_elastic.py psql_to_elastic.py
COPY .. .

ENTRYPOINT ["python", "psql_to_elastic.py" ]

