"""
    За основу взят пример из теории, в котором из базы выгружались все фильмы,
    у которых update > {назначенной даты}.
    Этот запрос был дважды изменен: для жанров и людей. В обоих случаях поле update берется
    из соответствующих таблиц: genres и persons соответственно.
    При этом каждый запрос проверяет на наличие uuid фильма в предыдущих выгрузках.
    Для этого на вход дополнительно передается set из uuid фильмов set_uuid
"""


def get_films(modified):
    return f"""SELECT
               fw.id,
               fw.title,
               fw.description,
               fw.rating,
               fw.type,
               fw.created,
               fw.modified,
               COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'person_role', pfw.role,
                           'person_id', p.id,
                           'person_name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
               ) as persons,
               array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.modified > TIMESTAMP '{modified}' or 
            pfw.created > TIMESTAMP '{modified}' or 
            gfw.created > TIMESTAMP '{modified}' or 
            p.modified > TIMESTAMP '{modified}' or 
            g.modified > TIMESTAMP '{modified}'
            GROUP BY fw.id
            ORDER BY fw.modified"""
