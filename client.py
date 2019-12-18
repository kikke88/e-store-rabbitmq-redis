import pika
import sys
import threading
import json

def is_int(input):
    try:
        num = int(input)
    except ValueError:
        return False
    return True
    
def recieve_message_from_server(my_id):
	def callback(ch, method, properties, body):
		ans = json.loads(body)
		if ans == 'STOP':#  duck tape for compliting client session
			exit(1)
		print('--------------')
		print(ans, end='')
		print('--------------')
		return

	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat = 0))
	channel_server_to_client = connection.channel()
	channel_server_to_client.queue_declare(queue=my_id + '_queue')
	channel_server_to_client.basic_consume(queue=my_id + '_queue', on_message_callback=callback, auto_ack=True)
	channel_server_to_client.start_consuming()
	return


my_id = sys.argv[1]
thread = threading.Thread(target = recieve_message_from_server, args = (my_id,))
thread.start()

connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost', heartbeat = 0))
channel = connection.channel()
channel.exchange_declare(exchange = 'server_exchange', exchange_type = 'fanout')
flag = True
command_list = ['help', 'add_to_cart', 'rem_from_cart', 'buy', 'discard', 'show_all', 'show_cart', 'list_category', 'show_category', 'get_stat']
get_stat_arg = ['cost', 'made_purchase', 'total_customers', 'avg_num_of_diff_prod'] 
while flag:
	request = input().split()
	if 	(len(request) == 0 or 
		request[0] not in command_list or
		request[0] == 'add_to_cart' and len(request) != 3 or
		request[0] in ['get_stat', 'rem_from_cart', 'show_category'] and len(request) != 2 or
		request[0] in ['help', 'buy', 'discard', 'show_all', 'list_category', 'show_cart'] and len(request) != 1 or
		request[0] == 'get_stat' and request[1] not in get_stat_arg or
		request[0] == 'add_to_cart' and is_int(request[2]) == False or
		request[0] == 'add_to_cart' and is_int(request[2]) == True and int(request[2]) <= 0):
		print('--------------\nBAD COMMAND\n--------------')
	elif request[0] == 'help':
		print(	'add_to_cart *item number* *quantity of items*\n',
				'rem_from_cart *item_num*\n',
				'buy\n',
				'discard\n',
				'show_all\n',
				'show_cart\n'
				'list_category\n',
				'show_category *category*\n',
				'get_stat cost/made_purchase/total_customers/avg_num_of_diff_prod\n', 
				'--------------', sep='')
	else:
		if request[0] == 'buy':
			flag = False
		request.insert(0, my_id)
		channel.basic_publish(exchange = 'server_exchange', routing_key = '', body = json.dumps(request))

thread.join()
