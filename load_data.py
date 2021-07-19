import pymysql


def title_basics():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.basics.tsv'
    f = open(filename, "r")

    insert_sql = """insert into content (cno, type, primary_title, original_title, is_adult, start_year, end_year, runtime)
                    values(%s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_sql1 = """insert into genres (cno, genre)
                    values(%s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows = []
    rows1 = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = attr[0][2:]
        type_ = attr[1]
        p_title = attr[2]
        o_title = attr[3]
        is_adult = attr[4]
        s_year = attr[5]
        e_year = attr[6]
        runtime = attr[7]
        if attr[8] != '\\N':
            genres = tuple(attr[8].split(','))
            for k in genres:
                t1 = [cno, k]
                rows1.append(t1)

        t = [cno, type_, p_title, o_title, is_adult, s_year, e_year, runtime]

        rows.append(t)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            cur.executemany(insert_sql1, rows1)
            conn.commit()
            print("title_basics : %d rows" % i)
            rows = []
            rows1 = []
        one_line = f.readline()[:-1]

    if rows:
        cur.executemany(insert_sql, rows)
        cur.executemany(insert_sql1, rows1)

        conn.commit()
        print("title_basics : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def name_basics():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'name.basics.tsv'
    f = open(filename, "r")

    insert_sql1 = """insert into participant (pno, name, birth_year, death_year)
                    values(%s, %s, %s, %s)"""
    insert_sql2 = """insert into profession (pno, role)
                    values(%s, %s)"""
    insert_sql3 = """insert into masterpiece (pno, cno)
                    values(%s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows1 = []
    rows2 = []
    rows3 = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        pno = attr[0][2:]
        # print(pno)
        # print(int(pno))
        # if i != int(pno):
        #     continue
        name = attr[1]
        birth_year = attr[2]
        death_year = attr[3]
        if attr[4] != '\\N':
            profession = tuple(attr[4].split(','))
            for k in profession:
                t2 = [pno, k]
                rows2.append(t2)

        if attr[5] != '\\N':
            masterpiece = tuple(attr[5].split(','))
            for j in masterpiece:
                t3 = [pno, j[2:]]
                rows3.append(t3)

        t1 = [pno, name, birth_year, death_year]
        rows1.append(t1)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql1, rows1)
            cur.executemany(insert_sql2, rows2)
            cur.executemany(insert_sql3, rows3)
            conn.commit()
            print("name_basics : %d rows" % i)
            rows1 = []
            rows2 = []
            rows3 = []
        one_line = f.readline()[:-1]

    if rows1:
        cur.executemany(insert_sql1, rows1)
        cur.executemany(insert_sql2, rows2)
        cur.executemany(insert_sql3, rows3)
        conn.commit()
        print("name_basics : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def title_akas():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.akas.tsv'
    f = open(filename, "r")

    insert_sql = """insert into titles (cno, ordering, title, region, language , type, attr, is_origin_title)
                        values(%s, %s, %s, %s, %s, %s, %s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = attr[0][2:]
        ordering = attr[1]
        title = attr[2]
        region = attr[3]
        language = attr[4]
        type_ = attr[5]
        attr_ = attr[6]
        is_origin_title = attr[7]
        t = [cno, ordering, title, region, language, type_, attr_, is_origin_title]

        rows.append(t)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("title_akas : %d rows" % i)
            rows = []
        one_line = f.readline()[:-1]

    if rows:
        cur.executemany(insert_sql, rows)
        conn.commit()
        print("title_akas : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def title_crew():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.crew.tsv'
    f = open(filename, "r")

    insert_sql1 = """insert into director (cno, pno)
                    values(%s, %s)"""
    insert_sql2 = """insert into writer (cno, pno)
                    values(%s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows1 = []
    rows2 = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = attr[0][2:]

        if attr[1] != '\\N':
            director = tuple(attr[1].split(','))
            for j in director:
                t1 = [cno, j[2:]]
                rows1.append(t1)

        if attr[2] != '\\N':
            writer = tuple(attr[2].split(','))
            for k in writer:
                t2 = [cno, k[2:]]
                rows2.append(t2)

        i += 1

        if i % 10000 == 0:
            cur.executemany(insert_sql1, rows1)
            cur.executemany(insert_sql2, rows2)
            conn.commit()
            print("title_crew : %d rows" % i)
            rows1 = []
            rows2 = []
        one_line = f.readline()[:-1]

    if rows1:
        cur.executemany(insert_sql1, rows1)
        conn.commit()
        print("title_crew : %d rows" % i)

    if rows2:
        cur.executemany(insert_sql2, rows2)
        conn.commit()
        print("title_crew : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def title_episode():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.episode.tsv'
    f = open(filename, "r")

    insert_sql = """insert into episode (cno, p_cno, season, episode)
                    values(%s, %s, %s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = attr[0][2:]
        p_cno = attr[1][2:]
        season = attr[2]
        episode = attr[3]
        t = [cno, p_cno, season, episode]
        rows.append(t)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("title_episode : %d rows" % i)
            rows = []
        one_line = f.readline()[:-1]

    if rows:
        cur.executemany(insert_sql, rows)
        conn.commit()
        print("title_episode : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def title_principals():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.principals.tsv'
    f = open(filename, "r")

    insert_sql = """insert into principal (cno, ordering, pno, category, job, characters)
                    values(%s, %s, %s, %s, %s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = attr[0][2:]
        ordering = attr[1]
        pno = attr[2][2:]
        category = attr[3]
        job = attr[4]
        characters = attr[5]

        t = [cno, ordering, pno, category, job, characters]
        rows.append(t)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("title_principals : %d rows" % i)
            rows = []
        one_line = f.readline()[:-1]

    if rows:
        cur.executemany(insert_sql, rows)
        conn.commit()
        print("title_principals : %d rows" % i)

    f.close()
    cur.close()
    conn.close()


def title_ratings():
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)
    filename = 'title.ratings.tsv'
    f = open(filename, "r")

    insert_sql = """insert into review (cno, avg_rating, num_votes)
                    values(%s, %s, %s)"""

    one_line = f.readline()
    one_line = f.readline()[:-1]
    rows = []
    i = 0

    while one_line:
        attr = tuple(one_line.split('\t'))
        cno = int(attr[0][2:])
        avg = float(attr[1])
        num = int(attr[2])
        t = [cno, avg, num]
        rows.append(t)
        i += 1
        if i % 10000 == 0:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print("title_ratings : %d rows" % i)
            rows = []
        one_line = f.readline()[:-1]

    if rows:
        cur.executemany(insert_sql, rows)
        conn.commit()
        print("title_ratings : %d rows" % i)

    f.close()
    cur.close()
    conn.close()