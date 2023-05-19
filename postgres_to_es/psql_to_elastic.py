import datetime
import logging
import psycopg2
import sql_requests
import os
from backoff import backoff
from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from schema import schema
from state import State, JsonFileStorage
from time import sleep

load_dotenv()


dsl = {
    'dbname': os.environ.get("DB_NAME"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': os.environ.get("DB_HOST"),
    'port': 5432
}


class ETL:
    @staticmethod
    @backoff(0.1, 2, 10)
    def start(first_start=True):
        file = JsonFileStorage(os.environ.get("STATE_FILE"))  # JSON файл для хранения состояния
        state = State(file)
        if first_start:  # Если это первый запуск, устанавливаем дату по умолчанию
            state.set_state(key="last_modified", value=datetime(1900, 1, 1, 0, 0))
        try:
            # Достаем дату последнего изменения из JSON файла
            last_modified = state.get_state(key="last_modified")
            with psycopg2.connect(**dsl) as connection:
                cursor = connection.cursor()
                psql_extr = PostgresExtractor(cursor, last_modified)
                psql_extr.start_looking_for_updates()  # Достаем все измененные фильмы
                last_modified = datetime.now()  # Сразу фиксируем дату последней выгрузки фильмов
                loader = ElasticsearchLoader(hosts=[os.environ.get("ES_HOST")])
                loader.save_to_es(psql_extr.return_es_data())
                # При успешной записи в ES сохраняем дату в JSON:
                state.set_state(key="last_modified", value=last_modified)
                sleep(int(os.environ.get("SLEEP_TIME")))
                restarter = ETL()
                restarter.start(first_start=False)
            return True
        except Exception as err:
            raise err


class PostgresExtractor:
    def __init__(self, cursor, last_modified):
        self.cursor = cursor
        self.modified = last_modified
        self.set_of_films = set()
        self.data_to_load = list()

    def start_looking_for_updates(self):
        self.get_data(sql_requests.get_films(self.modified))  # Выгрузка фильмов

    def get_data(self, query: str):
        self.cursor.execute(query)
        self.data_to_load.extend(self.cursor.fetchall())
        self.set_of_films.update([f[0] for f in self.data_to_load])

    def return_es_data(self):
        index = 0
        for row in self.data_to_load:
            index += 1
            yield {
                '_id': row[0],
                'id': row[0],
                'imdb_rating': row[3],
                "genre": row[-1],
                "title": row[1],
                "description": row[2],
                "director": [i["person_name"] for i in row[-2] if i["person_role"] == 'director'],
                "actors_names": [i["person_name"] for i in row[-2] if i["person_role"] == 'actor'],
                "writers_names": [i["person_name"] for i in row[-2] if i["person_role"] == 'writer'],
                "actors": [{"id": i["person_id"], "name": i["person_name"]}
                           for i in row[-2] if i["person_role"] == 'actor'],
                "writers": [{"id": i["person_id"], "name": i["person_name"]}
                            for i in row[-2] if i["person_role"] == 'writer'],
            }


class ElasticsearchLoader:
    def __init__(self, hosts):
        self.es = Elasticsearch(hosts=hosts)
        if not self.es.ping():
            raise ConnectionError("Failed to connect to Elasticsearch")
        if not self.es.indices.exists(index="movies"):
            self.es.indices.create(index="movies", body=schema)
        logging.warning("INDEX IS MADE")

    def save_to_es(self, generator):
        bulk(
            client=self.es,
            index="movies",
            actions=generator,
            raise_on_error=True,
        )


if __name__ == "__main__":
    etl = ETL()
    etl.start()

