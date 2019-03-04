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

parser = argparse.ArgumentParser(__file__, description="Fake Websale Transactions Data Generator")
#parser.add_argument("--output", "-o", dest='output_type', help="Write JSON, Avro or ProtoBuf data to a file or to STDOUT", choices=['AVRO','PROTO','JSON','CONSOLE'] )
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)

args = parser.parse_args()

log_lines = args.num_lines
#output_type = args.output_type

faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()

cc_types = ['visa','mastercard','amex','discover','diners']
ualist = [faker.firefox(), faker.chrome(), faker.safari(), faker.internet_explorer(), faker.opera()]
service_list = ['manual', 'amazon','dhl','fedex']
cities = ['Hampden', 'Essex', 'Bristol', 'Barnstable', 'Suffolk', 'Norfolk', 'Plymouth', 'Middlesex']


#outFileName = 'data_file_'+timestr+'.data'

f = None
try:
    '''
    if output_type == 'LOG':
        f = open(outFileName,'w')
    elif output_type == 'GZ':
        f = gzip.open(outFileName+'.gz','w')
    elif output_type == 'CONSOLE':
        pass
    else:
        f = sys.stdout
    '''

    # Create a bunch of session ids
    sessions = []
    for x in range(0, 100):
        sessions.append(faker.sha1(raw_output=False))

    # Create a bunch of product ids
    product_ids = []
    for x in range(0, 100):
        product_ids.append(faker.msisdn())

    f = sys.stdout
    flag = True
    while (flag):

        cc_type = random.choice(cc_types)

        purchase_amount = round(random.uniform(1, 1000),2)

        billing_address = {}
        billing_address['address1'] = faker.street_address()
        billing_address['address2'] = faker.secondary_address()
        billing_address['city'] = random.choice(cities)
        billing_address['company'] = faker.company()

        creditcard_info = {}
        creditcard_info['transaction_date'] = faker.date_this_year(before_today=True, after_today=False).strftime("%m/%d/%Y")
        creditcard_info['card_number'] = faker.credit_card_number(card_type=cc_type)
        creditcard_info['card_expiry_date'] = faker.credit_card_expire(start="now", end="+10y", date_format="%m/%y")
        creditcard_info['card_security_code'] = faker.credit_card_security_code(card_type=cc_type)
        creditcard_info['purchase_amount'] = purchase_amount
        creditcard_info['description'] = faker.company()

        client_details = {}
        client_details['browser_ip'] = faker.ipv4()
        client_details['user_agent'] = random.choice(ualist)
        client_details['session_id'] = random.choice(sessions)


        fulfillment = {}

        fulfillment['fulfillable_quantity'] = 1
        fulfillment['fulfillment_service'] = random.choice(service_list)
        fulfillment['fulfillment_status'] = 'fulfilled'
        fulfillment['grams'] = round(random.uniform(1, 1000),2)
        fulfillment['id'] = faker.ean13()
        fulfillment['total_price'] = round(purchase_amount,2)

        products = {}

        products['product_id'] = random.choice(product_ids)
        products['quantity'] = random.randint(1, 10)
        products['requires_shipping'] = 1

        fulfillment['products'] = products

        shopper_profile = {}

        shopper_profile['name'] = faker.name_female()
        shopper_profile['username'] = faker.user_name()
        shopper_profile['sex']='F'
        shopper_profile['mail']= faker.ascii_safe_email()

        order = {}
        order['billing_address'] = billing_address
        order['credit_card'] = creditcard_info
        order['web_client_details'] = client_details
        order['fulfillment'] = fulfillment
        order['shopper'] = shopper_profile
        order['buyer_accepts_marketing'] = faker.boolean(chance_of_getting_true=50)
        order['cart_token'] = faker.uuid4()

        print(json.dumps(order))
        #f.write(json.dumps(order))

#producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=json.loads)
#pkt = json.dumps(order)
#producer.send('TestRun', pkt)

        log_lines = log_lines - 1
        flag = False if log_lines == 0 else True

finally:
    if f is not None:
        f.close()
