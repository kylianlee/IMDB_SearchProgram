import load_data
import pymysql
import datetime


def print_menu():
    print("""
    ====================================================
    1. 영화 검색
    2. 특정 배우가 등장하는 영화의 별점이 높은 순으로 검색
    3. 특정 감독이 제작한 영화를 개봉연도 순으로 검색
    4. 드라마 장르의 영화를 리뷰가 많은 순 또는 별점이 높은순으로 검색
    5. 2020년 이후로 개봉한 우리나라 영화 중 상영시간이 120분이 넘는 영화 제목과 개봉 연도와 상영시간
    6. 넷플릭스 드라마 킹덤의 평점과 투표수
    7. 말론 브란도가 출연한 작품들의 평점
    0. 종료
    ====================================================


>>>>>>""")


if __name__ == '__main__':
    # load_data.title_basics()
    # load_data.name_basics()
    # load_data.title_akas()
    # load_data.title_crew()
    # load_data.title_episode()
    # load_data.title_ratings()
    # load_data.title_principals()
    conn = pymysql.connect(host='localhost', user='root', password='2.71828ho', db='IMDB',
                           unix_socket='/tmp/mysql.sock')
    cur = conn.cursor(pymysql.cursors.DictCursor)

    while True:
        print_menu()
        try:
            num = int(input())
        except:
            print("잘못된 입력입니다!")
            continue
        if num == 0:
            break
        elif num == 1:
            print("영화제목 : ")
            title = input()
            sql = "select * from content where type = 'movie' and primary_title = '%s'" % title
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 2:
            print("배우이름 : ")
            actor = input()
            sql = """select distinct pc.name, c.primary_title, r.avg_rating
                    from participant pc, profession pf, masterpiece m, content c, review r
                    where pc.pno = pf.pno and pc.pno = m.pno and m.cno = c.cno and c.cno = r.cno and pc.name = '%s'
                    order by r.avg_rating desc""" % actor
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 3:
            print("감독이름 : ")
            producer = input()
            sql = """select distinct p.name, c.primary_title, r.avg_rating
                    from director d, participant p, content c, review r
                    where p.pno = d.pno and c.cno = d.cno and c.cno = r.cno and c.type = 'movie' and p.name = '%s'
                    order by r.avg_rating desc""" % producer
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 4:
            sql = """select distinct c.primary_title, r.avg_rating, r.num_votes
                    from content c, genres g, review r
                    where c.cno = g.cno and c.cno = r.cno and g.genre = 'drama' and c.type = 'movie'
                    order by r.num_votes desc, r.avg_rating desc
            """
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 5:
            sql = """select c.primary_title, c.start_year, c.runtime
                    from content c, titles t
                    where c.cno = t.cno and t.region = 'kr' and c.start_year >= 2020 and c.type = 'movie' and runtime >= 120
            """
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 6:
            sql = """select c.primary_title, r.avg_rating, r.num_votes
                    from content c, review r, titles t
                    where c.cno = r.cno and c.cno = t.cno and t.region = 'kr' and c.primary_title = 'kingdom' and c.start_year = 2019
            """
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        elif num == 7:
            sql = """select c.primary_title, r.avg_rating
                    from participant p, review r, masterpiece m, content c
                    where p.pno = m.pno and c.cno = m.cno and m.cno = r.cno and p.name = 'marlon brando'
                    order by r.avg_rating desc
            """
            start_time = datetime.datetime.now()
            cur.execute(sql)
            rows = cur.fetchall()
            end_time = datetime.datetime.now()
            for row in rows:
                print(row)
            print("수행 시간 : %s" % (end_time - start_time))
        else:
            print("잘못된 입력입니다!")
