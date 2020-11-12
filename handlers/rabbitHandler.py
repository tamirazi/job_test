import pika ,json
from configs.appConfig import AMQP

class rabbitHandler:

    def __init__(self, route):
        """Initialize class.

        Args:
            route : queue name.
        """
        self.conn=None
        self.ch= None
        self.queue = None
        self.route = route
    
    def connect(self):
        """Connect to rabbitmq specipic queue (route)."""
        try:
            parameters = pika.URLParameters(AMQP)
            self.conn =  pika.BlockingConnection(parameters)
            self.ch = self.conn.channel()
            self.queue = self.ch.queue_declare(queue=self.route)
            print("connected to rabbit -> " + self.route)
        except :
            print ('rabbitmq connection to error')

    def disconnect(self):
        """Disconnect from rabbitmq specipic queue (route)."""
        if self.conn:
            # self.ch.queue_delete(queue=self.route)
            self.ch.close()
            self.conn.close()
            print("disconnected from rabbit -> " + self.route)

    def sendMsg(self, msg):
        """Publish message  from rabbitmq specipic queue (route)."""
        self.ch.basic_publish(exchange='', routing_key=self.route, body=json.dumps(msg))

    def consume(self, callback):
        """Consume mesagges from a queue.
            Args:
                callback: callback function to execute when message arrived.
        
        """
        self.ch.basic_consume(queue=self.route, on_message_callback=callback, auto_ack=True)
        self.ch.start_consuming()