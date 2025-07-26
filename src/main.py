from sqlalchemy import create_engine, text, Column, Integer, UUID
from sqlalchemy.orm import DeclarativeBase, Session
import math

USERNAME = "alex"
PASSWORD = "21779835"
HOST = "localhost"
PORT = "5435"
DATABASE = "dnstesttask"


class Base(DeclarativeBase):
    pass

class NewTransit(Base):
    __tablename__ = 'new_transits'

    count = Column(Integer, autoincrement=True, primary_key=True)
    branch_id = Column(UUID)
    product_id = Column(UUID)
    number = Column(Integer)

class ProductDistribution:
    def __init__(self):
        self.engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        self.branches = self.get_branches()
        self.products = self.get_products()
        Base.metadata.drop_all(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        # self.categories = self.get_categories()


    def get_db_data(self, txt):
        with self.engine.connect() as connection:
            return connection.execute(text(txt)).fetchall()

    def get_branches(self):
        result = self.get_db_data("select * from current_branch_data")
        return {str(x[0]):{"branch": str(x[0]), "room": x[2] - x[1],
                           "available_percent": x[1] / x[2], "needs":x[1], "volume": x[2]}
                for x in result}

    def get_products(self):
        result = self.get_db_data("select distinct product_id from products_to_distribute")
        return result

    def readable_view(self, seq):
        result = dict()
        for item in seq:
            if not result.get(str(item[0])):
                result[str(item[0])] = list()
            result[str(item[0])].append({"product": str(item[0]),
                                    "branch": str(item[1]),
                                    "branch_remain": item[2],
                                    "stock_remain": item[3],
                                    "needs": item[4],
                                    "branch_priority": item[5],
                                    "branch_volume": item[6],
                                    "room_in_branch": item[7],
                                    })
        return result

    def insert_data(self, data):
        data_to_insert = [NewTransit(branch_id = x["branch_id"],
                                     product_id = x["product_id"],
                                     number = x["for_transit"]) for x in data]
        with Session(bind=self.engine) as session:
            session.add_all(data_to_insert)
            session.commit()

    def product_processing(self, product):
        new_transit = list()
        product_uuid = product[0].get("product")
        needs = sum([x.get("needs") for x in product])
        remains = product[0].get("stock_remain")
        if needs > 0:
            for branch in product:
                if remains > 0 and branch.get("needs") > 0:
                    branch_id = branch.get("branch")
                    prods_to_transit = math.ceil(branch.get("needs") * (1 - self.branches.get(branch_id).get("available_percent")))
                    if self.branches.get(branch_id).get("room") < prods_to_transit:
                        print(self.branches.get(branch_id).get("room"), branch_id)

                    if (prods_to_transit > 0) and self.branches.get(branch_id).get("room") >= prods_to_transit:
                        if prods_to_transit > remains:
                            new_transit.append({"product_id": product_uuid,
                                                "branch_id": branch_id,
                                                "for_transit": remains})
                            remains = 0
                        else:
                            new_transit.append({"product_id": product_uuid,
                                                "branch_id": branch_id,
                                                "for_transit": prods_to_transit})
                            remains -= prods_to_transit
                            self.branches[branch_id]["room"] -= prods_to_transit
        return new_transit

    def run_distribution(self):
        range_value = math.ceil(len(self.products) / 1000)
        for x in range(range_value):
            txt = (f"select * from products_to_distribute where product_id in "
                   f"(select product_id from rc_product offset {x * 1000} limit 1000) "
                   f"order by priority, branch_volume desc")
            result = self.readable_view(self.get_db_data(txt))

            new_transit = list()
            for product in result.values():
                new_transit += self.product_processing(product)

            self.insert_data(new_transit)


def main():
    pd = ProductDistribution()
    pd.run_distribution()


if __name__ == '__main__':
    main()
