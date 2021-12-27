import time

from config import *
import psycopg2

cursor = None
connection = None


def connect():
    try:
        global connection, cursor
        connection = psycopg2.connect(user=user, password=password, host='localhost', port="5432", database=database)

        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print("Can't connect to DB", error)
        disconnect()


def disconnect():
    if connection:
        cursor.close()
        connection.close()
        print("Successfully disconnected from DB")
    else:
        print("Can't disconnect")


def insert(num: int, col: list) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute("""INSERT INTO PUBLIC."Users" (nickname, status, date_registration) \
                          VALUES (%s, %s, %s)""", col)
            case 2:
                cursor.execute("""INSERT INTO PUBLIC."Questions" (topic, date, tags) \
                          VALUES (%s, %s, %s)""", col)
            case 3:
                cursor.execute("""INSERT INTO PUBLIC."Users/Questions" (userID, q_linkID) \
                          VALUES (%s, %s)""", col)
            case 4:
                cursor.execute("""INSERT INTO PUBLIC."Answers" (date, fk_qlinkID, answer, pos_rating, neg_rating) \
                          VALUES (%s, %s, %s)""", col)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Can't insert into table", error)
        cursor.execute('rollback')
        return False
    return True


def select(num: int, quantity: int = 100, offset: int = 0, id: str = "") -> list:
    if (cursor is None) and (connection is None):
        return []
    try:
        match num:
            case 1:
                if id:
                    cursor.execute("""SELECT * FROM public."Users" WHERE "userID"=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Users" ORDER BY "userID" ASC limit %s offset %s""",
                                   (quantity, offset,))
            case 2:
                if id:
                    cursor.execute("""SELECT * FROM public."Questions" WHERE "q_linkID"=%s""", id)
                else:
                    cursor.execute(
                        """SELECT * FROM public."Questions" ORDER BY "q_linkID" ASC limit %s offset %s""",
                        (quantity, offset,))
            case 3:
                if id:
                    cursor.execute("""SELECT * FROM public."Users/Questions" WHERE "userID"=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Users/Questions" ORDER BY "userID" ASC limit %s offset %s""",
                                   (quantity, offset,))
            case 4:
                if id:
                    cursor.execute("""SELECT * FROM public."Answers" WHERE "a_linkID"=%s""", id)
                else:
                    cursor.execute(
                        """SELECT * FROM public."Answers" ORDER BY "a_linkID" ASC limit %s offset %s""",
                        (quantity, offset,))
        return cursor.fetchall()
    except(Exception, psycopg2.Error) as error:
        print(f"Can't select from a table {num}, with {id=}\n", error)
        return []


def delete(num: int, id: str) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute("""DELETE FROM public."Users" WHERE "userID"=%s""", (id,))
            case 2:
                cursor.execute("""DELETE FROM public."Questions" WHERE "q_linkID" = %s""", (id,))
            case 3:
                cursor.execute("""DELETE FROM public."Users/Questions" WHERE "userID" = %s""", (id,))
            case 4:
                cursor.execute("""DELETE FROM public."Answers" WHERE "a_linkID" = %s""", (id,))
        connection.commit()
        return True
    except(Exception, psycopg2.Error) as error:
        print(f"Can't delete from a table {num}, {id=}\n", error)
        cursor.execute('rollback')
        return False


def update(num: int, col: list, id: int) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute(
                    """UPDATE PUBLIC."Users" SET nickname = %s, status = %s, date_registration = %s WHERE "userID"=%s;""",
                    (*col, id,))
            case 2:
                cursor.execute(
                    """UPDATE PUBLIC."Questions" SET topic = %s, date = %s, tags = %s WHERE "q_linkID" = %s;""",
                    (*col, id,))
            case 3:
                cursor.execute(
                    """UPDATE PUBLIC."Users/Questions" SET "userID" = %s, "q_linkID" = %s WHERE "userID" = %s;""",
                    (*col, id,))
            case 4:
                cursor.execute(
                    """UPDATE PUBLIC."Answers" SET date = %s, "fk_qlinkID" = %s, answer = %s, pos_rating = %s, neg_rating = %s, WHERE "a_linkID" = %s;""",
                    (*col, id,))
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Can't update the table row = {id}, {col}\n", error)
        cursor.execute('rollback')
        return False
    return True


def generate(num: int, quant: int):
    if (cursor is None) and (connection is None):
        return False
    try:
        for i in range(quant):
            match num:
                case 1:
                    cursor.execute(
                        """INSERT INTO public."Users"(nickname, status, date_registration) Select random_string(20), random_string(20),  NOW() + (random() * (NOW()+'90 days' - NOW())) + '30 days';""")
                case 2:
                    cursor.execute(
                        """INSERT INTO public."Questions"(topic, date, tags) Select random_string(50),  NOW() + (random() * (NOW()+'90 days' - NOW())) + '30 days', random_string(20);  """)
                case 3:
                    cursor.execute(
                        """INSERT INTO public."Users/Questions"("userID", "q_linkID") Select "userID", "q_linkID" From public."Users" cross join public."Questions" order by random() limit 1;""")
                case 4:
                    cursor.execute(
                        """INSERT INTO public."Answers"(answer, pos_rating, neg_rating, date, "fk_qlinkID") Select  random_string(50), random_between(1, 10),random_between(1, 10), ( NOW() + (random() * (NOW()+'90 days' - NOW())) + '30 days'), "q_linkID" From public."Questions" order by random() limit 1;""")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Can't insert in the table row = {num}\n", error)
        cursor.execute('rollback')
        return False
    return True


def search(tables: list[int], key1: str, key2: str, expression: str):
    sql_query = "select * from "
    match tables[0]:
        case 1:
            sql_query += 'public."Users" as first'
        case 2:
            sql_query += 'public."Questions" as first'
        case 3:
            sql_query += 'public."Users/Questions" as first'
        case 4:
            sql_query += 'public."Answers" as first'

    sql_query += ' inner join '
    match tables[1]:
        case 1:
            sql_query += 'public."Users" as second'
        case 2:
            sql_query += 'public."Questions" as second'
        case 3:
            sql_query += 'public."Users/Questions" as second'
        case 4:
            sql_query += 'public."Answers" as second'

    sql_query += f' on first."{key1}" = second."{key2}" Where {expression}'
    print('SQL QUERY =>', sql_query)
    global cursor
    try:
        timer = time.time_ns()
        cursor.execute(sql_query)
    except (Exception, psycopg2.Error) as error:
        print("Can't execute search\n", error)
        return []
    rows = cursor.fetchall()
    timer = time.time_ns() - timer
    return rows, timer
