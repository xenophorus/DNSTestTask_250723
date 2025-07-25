from django.db.models.expressions import result
from sqlalchemy import create_engine, text

USERNAME = "alex"
PASSWORD = "21779835"
HOST = "localhost"
PORT = "5435"
DATABASE = "dnstesttask"

class ProductDistribution:
    def __init__(self):
        self.engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        self.branches_num = self.get_branches()
        self.products_num = self.get_products()
        self.run_distribution()

    def get_db_data(self, txt):
        with self.engine.connect() as connection:
            return connection.execute(text(txt)).fetchall()


    def get_branches(self):
        result = self.get_db_data("select distinct branch_id from branch_product")
        return result[0][0]

    def get_products(self):
        result = self.get_db_data("select distinct product_id from rc_product where remain > 0")
        return result

    def run_distribution(self):
        for product in self.products_num:
            product_uuid = str(product[0])
            txt = f'''
            select * from products_to_distribute
            where product_id = '{product_uuid}'
            order by priority, branch_volume;
            '''
            result = self.get_db_data(txt)

            print(product)

# with engine.connect() as connection:
#     result = connection.execute(text("select * from stores"))
#     for row in result:
#         print(row)
#     print(1)



def main():
    pd = ProductDistribution()



if __name__ == '__main__':
    main()

#alex dnstesttask