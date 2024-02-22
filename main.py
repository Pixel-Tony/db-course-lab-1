from random import randint
from threading import Thread
import time as t

try:
    import psycopg2 as pg
    from psycopg2.extensions import connection, cursor
except ImportError as err:
    print(f"Для виконання необхідно встановити модуль '{err.name}'")
    quit()

DB_NAME = 'course_db'   # Ім'я бази даних у СУБД
DB_USER = 'student01'   # Ім'я користувача СУБД
DB_PASSWORD = '123'     # Пароль користувача СУБД

DB_DSN = f'dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}'
N_THREADS = 4           # Кількість потоків
ITER_COUNT = 10_00      # Кількість ітерацій у одному потоці
ROW_COUNT = 100_000     # Кількість рядків, які створювати у БД


def create_table_tmpl(count: int): return f"""
CREATE TABLE IF NOT EXISTS user_counter(
    user_id         SERIAL      PRIMARY KEY ,
    counter         INT                     ,
    id_but_varchar  VARCHAR(10)             ,
    version         INT
);

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..{count} LOOP
        INSERT INTO user_counter(user_id, counter, version, id_but_varchar)
        VALUES (i, 0, 0, i::varchar);
    END LOOP;
END
$$;
COMMIT;
"""


def init_table(cur: cursor):
    cur.execute(create_table_tmpl(ROW_COUNT))


def truncate_table(cur: cursor):
    cur.execute('TRUNCATE TABLE IF EXISTS user_counter')


def drop_table(cur: cursor):
    cur.execute('DROP TABLE IF EXISTS user_counter')


def bind_benchmark(dsn: str, n_threads: int, count: int):
    def _wrap(f):
        def target():
            with pg.connect(dsn) as conn, conn.cursor() as cur:
                f(conn=conn, cur=cur, count=count)

        print(f"Виконується {f.__name__}...")

        threads = [Thread(None, target) for _ in range(n_threads)]

        t1 = t.perf_counter()
        [t.start() for t in threads]
        [t.join() for t in threads]
        t2 = t.perf_counter()

        with pg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute('SELECT SUM(counter) '
                        'FROM user_counter')
            conn.commit()
            counter, = cur.fetchone()
            cur.execute('UPDATE user_counter '
                        'SET counter = 0')
            conn.commit()

        print(f"Виконання зайняло {t2 - t1:.4f}с;")
        print(f"Сума у стовпці counter: {counter};")
        print()
        return (f.__name__, t2 - t1)

    return lambda f: (lambda *args, **kwgs: _wrap(f, args, kwgs))


@bind_benchmark(DB_DSN, N_THREADS, ITER_COUNT)
def lost_update(conn: connection, cur: cursor, count: int):
    for _ in range(count):
        cur.execute('SELECT counter '
                    'FROM user_counter '
                    'WHERE user_id = 1')
        counter, = cur.fetchone()
        counter += 1
        cur.execute('UPDATE user_counter '
                    'SET counter = %s '
                    'WHERE user_id = 1',
                    [counter])
        conn.commit()


@bind_benchmark(DB_DSN, N_THREADS, ITER_COUNT)
def in_place_update(conn: connection, cur: cursor, count: int):
    for _ in range(count):
        cur.execute('UPDATE user_counter '
                    'SET counter = counter + 1 '
                    'WHERE user_id = 1')
        conn.commit()


@bind_benchmark(DB_DSN, N_THREADS, ITER_COUNT)
def rowlevel_lock(conn: connection, cur: cursor, count: int):
    for _ in range(count):
        cur.execute('SELECT counter '
                    'FROM user_counter '
                    'WHERE user_id = 1 '
                    'FOR UPDATE')
        counter, = cur.fetchone()
        counter += 1
        cur.execute('UPDATE user_counter '
                    'SET counter = %s '
                    'WHERE user_id = 1',
                    [counter])
        conn.commit()


@bind_benchmark(DB_DSN, N_THREADS, ITER_COUNT)
def OCC(conn: connection, cur: cursor, count: int):
    for _ in range(count):
        while True:
            cur.execute('SELECT counter, version FROM user_counter '
                        'WHERE user_id = 1')
            counter, version = cur.fetchone()
            counter += 1
            cur.execute('UPDATE user_counter '
                        'SET counter = %s, version = %s '
                        'WHERE user_id = 1 AND version = %s',
                        [counter, version + 1, version])
            conn.commit()
            if cur.rowcount > 0:
                break


def first_course():
    print('Кожен запит буде виконано '
          f'у {N_THREADS} потоках по {ITER_COUNT} разів;')

    results = dict([
        lost_update(),
        in_place_update(),
        rowlevel_lock(),
        OCC()
    ])

    results = sorted(results.items(), key=lambda p: p[1])
    longest_name = len(max(results, key=lambda p: len(p[0]))[0])
    fstr = f'{{name: >{longest_name + 1}}} -> {{time_taken:.2f}}с.'
    lines = [
        fstr.format(name=name, time_taken=time_taken)
        for name, time_taken in results
    ]
    L = max(len(line) for line in lines)

    HR = '='*L
    print('\n'.join((
        HR,
        f'{{: ^{L}}}'.format('Результати бенчмаркінгу'),
        HR,
        *lines,
        HR,
        ''
    )))


@bind_benchmark(DB_DSN, N_THREADS, ITER_COUNT)
def in_place_update_by_varchar(conn: connection, cur: cursor, count: int):
    for _ in range(count):
        cur.execute('UPDATE user_counter '
                    'SET counter = counter + 1 '
                    'WHERE id_but_varchar = %s',
                    [str(randint(1, ROW_COUNT))])
        conn.commit()


def dessert():
    without_index = in_place_update_by_varchar()[1]

    print('Створимо індекс по полю типу varchar...')
    with pg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute('CREATE INDEX IF NOT EXISTS test_index '
                    'ON user_counter(id_but_varchar)')
    print('Індекс створено;\n')

    with_index = in_place_update_by_varchar()[1]

    print(f'Оновлення випадкових значень без індексу -> {without_index:.2f}')
    print(f'            - // -           з індексом  -> {with_index:.2f}')

    with pg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute('DROP INDEX IF EXISTS test_index')


def intro():
    print('\n'.join((
        'ЛАБОРАТОРНА РОБОТА №1 з дісципліни «БАЗИ ДАНИХ»',
        'Роботу виконав: Студент КМ-12',
        '                Пєшков Антон',
        'Київ, 2024'
    )))


def main():
    with pg.connect(DB_DSN) as conn, conn.cursor() as cur:

        drop_table(cur)
        init_table(cur)

        intro()

        print('ОСНОВНА ЧАСТИНА')
        first_course()

        print('ДОДАТКОВЕ ЗАВДАННЯ')
        dessert()

        drop_table(cur)


if __name__ == '__main__':
    main()
