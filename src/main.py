import psycopg2

from sqlalchemy import create_engine, text

engine = create_engine(f'postgresql://alex:21779835@localhost:5435/dnstesttask')

with engine.connect() as connection:
    result = connection.execute(text("select * from stores"))
    for row in result:
        print(row)
    print(1)



def main():
    ...


if __name__ == '__main__':
    main()

#alex dnstesttask