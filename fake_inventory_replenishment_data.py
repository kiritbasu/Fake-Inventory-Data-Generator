# Inventory Schema

#!/usr/bin/python
import time
import datetime
import pytz
import random
import sys
import argparse
from faker import Faker
from random import randrange
import json
import csv
import zipcodes

#from kafka import KafkaProducer

class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False

parser = argparse.ArgumentParser(__file__, description="Fake JSON/AVRO/ProtoBuf Data Generator")
parser.add_argument("--output", "-o", dest='output_type', help="Write JSON, Avro or ProtoBuf data to a file or to STDOUT", choices=['AVRO','PROTO','JSON','CONSOLE'] )
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)

args = parser.parse_args()

log_lines = args.num_lines
output_type = args.output_type

faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()

store_size = [180000,200000,210000,220000,250000,230000]
uom = ['box','pallet', 'pack', 'barrel', 'bag']
commercial_shipping = ['usps','dhl','fedex', 'ups']
zips = [55328, 55329, 55330, 55331, 55332, 55333, 55334, 55335, 55336, 55337, 55338, 55339, 55340, 55341, 55342, 55343, 55344, 55345, 55346, 55347, 55348, 55349, 55350, 55352, 55353, 55354, 55355, 55356, 55357, 55358, 55359, 55360, 55361, 55362, 55363, 55364, 55365, 55366, 55367, 55368, 55369, 55370, 55371, 55372, 55373, 55374, 55375, 55376, 55377, 55378, 55379, 55380, 55381, 55382, 55383, 55384, 55385, 55386, 55387, 55388, 55389, 55390, 55391, 55392, 55393, 55394, 55395, 55396, 55397, 55398, 55399, 55401]

f = None

try:

    f = sys.stdout
    stores = {}
    for i in range(0,30):
        st = {}
        st['id'] = faker.ean8()
        st['zip'] = random.choice(zips)
        st['size'] = random.choice(store_size)
        stores[i] = st

    products = {}
    for i in range(0,1000):
        prod_min_ord_qty = int(random.uniform(1, 10))
        prod_max_ord_qty = int(random.uniform(10, 100))

        prod_quantity = int(random.uniform(prod_min_ord_qty, prod_max_ord_qty))
        prod_unitprice = round(random.uniform(1, 50),2)
        prod_unitweight = round(random.uniform(1, 100),0)
        prod_package_weight = prod_quantity * prod_unitweight
        prod_onhand_quantity = int(random.uniform(10, 100))

        prod = {}
        prod['id'] = faker.ean13()
        prod['unit_of_measure'] = random.choice(uom)
        prod['order_quantity'] = prod_quantity
        prod['onhand_quantity'] = prod_onhand_quantity
        prod['unit_weight'] = prod_unitweight
        prod['package_weight'] = prod_package_weight
        prod['unit_price'] = prod_unitprice
        prod['product_lead_time'] = int(random.uniform(1, 30))
        prod['minimum_order_quantity'] = prod_min_ord_qty
        prod['maximum_order_quantity'] = prod_max_ord_qty
        prod['average_monthly_usage'] = int(random.uniform(150, 700))
        products[i] = prod

    flag = True
    while (flag):


        store = random.choice(stores)
        product = random.choice(products)

        order = {}
        order['order_date_time'] = str(faker.date_time_this_year(before_now=True, after_now=False))
        order['store_id'] = store['id']
        order['store_zip'] = store['zip']

        for case in switch(order['store_zip']):
            if case('55328'):
                order['store_lat'] = '45.041472'
                order['store_lon'] = '-93.97792'
                order['store_city'] = 'Delano'
                order['store_state'] = 'MN'
                break
            if case('55395'):
                order['store_lat'] = '44.946121'
                order['store_lon'] = '-94.07572'
                order['store_city'] = 'Winsted'
                order['store_state'] = 'MN'
                break
            if case(): pass

        order['store_size'] = store['size']
        order['product_id'] = product['id']
        order['unit_of_measure'] = product['unit_of_measure']
        order['order_quantity'] = product['order_quantity']
        order['onhand_quantity'] = product['onhand_quantity']
        order['unit_weight'] = product['unit_weight']
        if log_lines % 2000:
            order['package_weight'] = product['package_weight']

        order['unit_price'] = product['unit_price']
        order['product_lead_time'] = product['product_lead_time']
        order['minimum_order_quantity'] = product['minimum_order_quantity']
        order['maximum_order_quantity'] = product['maximum_order_quantity']
        order['average_monthly_usage'] = product['average_monthly_usage']

        #producer = KafkaProducer(bootstrap_servers='172.17.0.2:9092,172.17.0.3:9092,172.17.0.4:9092', #value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        #producer.send(b'Inventory', json.dumps(product))
        print(json.dumps(order))
        #f.write(json.dumps(order))
        #print (order)
        log_lines = log_lines - 1
        flag = False if log_lines == 0 else True

finally:
    if f is not None:
        f.close()
