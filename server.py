import pika
import sys
import json
import redis

bigHash = redis.Redis(db=48)

bigHash.flushall()

bigHash.hmset("1010", {'product_name':'Rubik\'s cube', 'quantity':'4', 'cost':'20', 'category':'Puzzle'})
bigHash.hmset("1001", {'product_name':'OnePlus3t', 'quantity':'2', 'cost':'637', 'category':'Phone|Black'})
bigHash.hmset("0101", {'product_name':'Intel i5 m460', 'quantity':'1', 'cost':'202', 'category':'Processor'})
bigHash.hmset("0010", {'product_name':'Pilot\t', 'quantity':'13', 'cost':'4', 'category':'Pen|Blue'})
bigHash.hmset("1111", {'product_name':'Asus F53s', 'quantity':'1', 'cost':'388', 'category':'Notebook|Black'})

categoryHash=redis.Redis(db=49)

for key in bigHash.keys():
	cur_category = bigHash.hget(key, "category").decode("utf-8")
	it = 0
	while it != -1:
		lable = str(cur_category[it:]).find('|')
		if lable == -1:
			ans = categoryHash.hget(cur_category[it:], "items")
			if ans == None:
				ans = key
			else:
				ans = ans + key
			categoryHash.hset(cur_category[it:], "items", ans)
			it = lable
		else:
			ans = categoryHash.hget(cur_category[it:], "items")
			if ans == None:
				ans = key
			else:
				ans = ans + key
			categoryHash.hset(cur_category[it:lable], "items", ans)
			it = lable + 1

dict_with_consumers_basket = {}
dict_with_chanels = {}
total_customers = 0
made_purchase = 0

#string
# .encode('utf-8')
#bytes
# .decode('utf-8')

def callback(ch, method, properties, body):
	global total_customers # problem with parameter passing to callback function
	global made_purchase
	rec_mes = json.loads(body)
	print(rec_mes)
	chnl_in_dict = dict_with_chanels.get(rec_mes[0])
	if chnl_in_dict == None:
		total_customers += 1
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		channel_server_to_client = connection.channel()
		channel_server_to_client.queue_declare(queue=rec_mes[0] + '_queue')
		dict_with_chanels[rec_mes[0]] = channel_server_to_client
		dict_with_consumers_basket[rec_mes[0]] = redis.Redis(db=int(rec_mes[0]))
	ans = ''
	if rec_mes[1] == 'add_to_cart':
		if rec_mes[2].encode('utf-8') not in bigHash.keys():
			ans = 'There is no product with this item number\n'
		elif int(rec_mes[3]) > int(bigHash.hget(rec_mes[2], "quantity")):
			ans = 'Not enough product. Available: ' + bigHash.hget(rec_mes[2], "quantity").decode("utf-8") + '\n'
		else:
			dict_with_consumers_basket[rec_mes[0]].hset(rec_mes[2], "quantity", rec_mes[3])
			ans = 'Done\n'
	elif rec_mes[1] == 'rem_from_cart':
		if dict_with_consumers_basket[rec_mes[0]].hexist(rec_mes[2], "quantity") == False:
			ans = "This product is not in backet"
		else:
			dict_with_consumers_basket[rec_mes[0]].hdel(rec_mes[2], "quantity")
			ans = 'Done\n'
	elif rec_mes[1] == 'buy':
		made_purchase += 1
		ans = 'Products\n'
		products = ''
		flag = False
		total = 0
		keys = dict_with_consumers_basket[rec_mes[0]].keys()
		if len(keys) == 0:
			ans = 'It is a pity that You did not buy anything\n'
		else:
			for key in keys:
				wanted_quantity = int(dict_with_consumers_basket[rec_mes[0]].hget(key, "quantity"))
				in_stock = bigHash.hexists(key,'cost')
				if in_stock == False:
					flag = True
					continue
				in_stock = bigHash.hgetall(key)
				if wanted_quantity <= int(in_stock[b'quantity']):
					products += str(wanted_quantity) + 'x' + in_stock[b'product_name'].decode('utf-8') + '\n'
					total += int(in_stock[b'cost']) * wanted_quantity
					if wanted_quantity == int(in_stock[b'quantity']):
						del_category = bigHash.hget(key, 'category').decode('utf-8')
						cur_key = key.decode('utf-8')
						it = 0
						list_w_category = []
						while it != -1:
							lable = del_category[it:].find('|')
							if lable == -1:
								list_w_category.append(del_category[it:])
								it = lable
							else:
								list_w_category.append(del_category[it:it + lable])
								it += lable + 1
						for del_c in list_w_category:
							str_w_items_num = categoryHash.hget(del_c, 'items').decode('utf-8')
							if len(str_w_items_num) == 4:
								categoryHash.hdel(del_c, 'items')
							else:
								items = [str_w_items_num[i:i + 4] for i in range(0, len(str_w_items_num), 4)]
								new_items = ''
								for item in items:
									if item != cur_key:
										new_items += item
								categoryHash.hset(del_c, 'items', new_items)
						bigHash.hdel(key, 'product_name', 'quantity', 'cost', 'category')
					else:
						bigHash.hincrby(key, 'quantity', -wanted_quantity)
				else:
					flag = True
			if flag == True:
				products += 'Note: One or more products have run out\n'
			ans += products + 'Total\n' + str(total) + '\n'
		dict_with_chanels[rec_mes[0]].basic_publish(exchange='', routing_key=rec_mes[0] + '_queue', body = json.dumps(ans))
		dict_with_chanels[rec_mes[0]].basic_publish(exchange='', routing_key=rec_mes[0] + '_queue', body = json.dumps('STOP'))
		return
	elif rec_mes[1] == 'discard':
		dict_with_consumers_basket[rec_mes[0]].flushdb()
		ans = 'Done\n'
	elif rec_mes[1] == 'show_all':
		for key in bigHash.keys():
			cur_product = bigHash.hvals(key)
			ans += (key + b'\t' + cur_product[0] + b'\t' + b'x' + cur_product[1] + b'\t' + cur_product[2] + b'\t' + cur_product[3] + b'\n').decode('utf-8')
	elif rec_mes[1] == 'list_category':
		for key in categoryHash.keys():
			ans += key.decode('utf-8') + '\n'
	elif rec_mes[1] == 'show_cart':
		keys = dict_with_consumers_basket[rec_mes[0]].keys()
		for key in keys:
			name = bigHash.hget(key, 'product_name')
			num = dict_with_consumers_basket[rec_mes[0]].hget(key, 'quantity')
			ans += (num + b'x' + name).decode('utf-8') + '\n'
	elif rec_mes[1] == 'show_category':
		if categoryHash.hexists(rec_mes[2], "items") == False:
			ans = 'Wrong category\n'
		else:
			field = categoryHash.hget(rec_mes[2], "items").decode('utf-8')
			keys = [field[i:i + 4] for i in range(0, len(field), 4)]
			for key in keys:
				cur_product = bigHash.hvals(key)
				ans += key + (b'\t' + cur_product[0] + b'\t' + b'x' + cur_product[1] + b'\t' + cur_product[2] + b'\t' + cur_product[3] + b'\n').decode('utf-8')
	elif rec_mes[1] == 'get_stat':
		if rec_mes[2] == 'cost':
			if len(dict_with_consumers_basket[rec_mes[0]].keys()) == 0:
				ans = 'Empty basket\n'
			else:
				num = 0
				total_cost = 0
				for key in dict_with_consumers_basket[rec_mes[0]].keys():
					cur_quantity = int(dict_with_consumers_basket[rec_mes[0]].hget(key, 'quantity'))
					cur_cost = int(bigHash.hget(key, 'cost'))
					num += cur_quantity
					total_cost += cur_cost * cur_quantity
				ans = str(total_cost / num) + '\n'
		elif rec_mes[2] == 'made_purchase':
			ans = str(made_purchase) + '\n'
		elif rec_mes[2] == 'avg_num_of_diff_prod':
			if len(dict_with_consumers_basket[rec_mes[0]].keys()) == 0:
				ans = 'Empty basket\n'
			else:
				num = 0
				list_w_keys = dict_with_consumers_basket[rec_mes[0]].keys()
				for key in list_w_keys:
					cur_quantity = int(dict_with_consumers_basket[rec_mes[0]].hget(key, 'quantity'))
					num += cur_quantity
				ans = str(num / len(list_w_keys)) + '\n'
		else:
			ans = str(total_customers) + '\n'
	else:
		print('AVOST')
		exit(1)
	dict_with_chanels[rec_mes[0]].basic_publish(exchange='', routing_key=rec_mes[0] + '_queue', body = json.dumps(ans))	

my_id = 'server'
connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
channel = connection.channel()
ex_name = 'server_exchange'
ex_que_name = 'server_exchange_queue'
channel.exchange_declare(exchange = ex_name, exchange_type = 'fanout')
result = channel.queue_declare(queue = ex_que_name, exclusive = True)
channel.queue_bind(exchange = ex_name, queue = ex_que_name)
channel.basic_consume(queue = ex_que_name, on_message_callback = callback, auto_ack = True)
channel.start_consuming()
