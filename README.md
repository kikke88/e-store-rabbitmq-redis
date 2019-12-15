## E-store

###General enunciation
Messages are forwarded via RabbitMQ. Data is stored in Redis. The buyer can view the goods, add to the cart, in the end get the shopping done.

###Data storage
Storing information in Redis data structure. Shopping cart and all products - in hash. Product hash: name - cost - quantity - article - category(can be several)

>Client <---> RabbitMQ <--->  Store logic <---> Redis

###Available commands:

* add_to_cart *(product article)*  *(quantity of products)*
* rem_from_cart *(item_num)*
* buy
* discard
* show_all
* show_cart
* list_category
* show_category *category*
* get_stat cost/made_purchase/total_customers/avg_num_of_diff_prod


Note: It is provided parallel work of several clients.

###How to run

Firstly, run

	rabbitmq-server
	redis-server
Then

	python server.py

and in another command lines	
	
	python client.py customer_id
customer id is in the ranges of 0 to 13
	