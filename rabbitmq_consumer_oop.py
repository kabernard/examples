# !/usr/bin/env python

"""
This is an OOP AMPQ consumer that will handle unexpected interactions with
RabbitMQ such as channel and connection closures. The consumer will exist for
60 seconds by default and shutdown cleanly.
Websites:
https://spark.apache.org/docs/latest/sql-programming-guide.html#json-datasets
https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html
http://spark.apache.org/docs/2.2.0/api/python/pyspark.html
https://media.readthedocs.org/pdf/pika/latest/pika.pdf
https://github.com/pika/pika/blob/master/pika/connection.py
Other Notes:
accountsDF.where("Name like '%John Doe%'").select(accountsDF['Id']).collect() # = [Row(Id=11)]
sitesDF.where("Name LIKE '%Site%s Name%'").select(sitesDF['Id']).collect() # = [Row(Id=12)]
"""


__author__ = 'Kwame Bernard'
__copyright__ = ''
__license__ = ''
__credits__ = ''
__version__ = "0.1a6"
__maintainer__ = "Kwame Bernard"
__email__ = "kwame.bernard@mac.com"
__status__ = "Prototype"  # "Prototype", "Development", "Production"

import pika
import time
import platform
import logging


# logging package levels:
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected.
# WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

if platform.system() == 'Darwin':
    filepath = '/Users/kwamebernard_work/PycharmProjects/test/sensor_incoming.json'  # local
    logpath = '/Users/kwamebernard_work/PycharmProjects/test/sensor_incoming_log.txt'
    file_handler = logging.FileHandler(logpath)
elif platform.system() == 'Linux':
    filepath = '/dbfs/tmp/sensor_incoming.json'  # dbfs
    logpath = '/dbfs/tmp/sensor_incoming_log.txt'  # dbfs
    file_handler = logging.FileHandler(logpath)
else:
    print('Filepath/Logger Error')

logger.addHandler(file_handler)

class RabbitmqConsumer:
    """
    This is a RabbitMQ consumer object
    """

    USER = 'username_dev'
    PASSWORD = '1@34%67*90'
    HOST = 'rabbitmq-server-name.rmq.cloudamqp.com'
    VIRTUAL_HOST = 'development'
    # QUEUE = 'incoming_sensor_name_dev'
    QUEUE = 'incoming_sensor_name'
    SSL = True
    PORT = 1234
    DURABLE = True
    ARGUMENTS = {"x-message-ttl": 60000}

    def __init__(self, filename, mode):
        """
        Create a new instance of the consumer class, used to connect to RabbitMQ.
        """
        self.file = open(filename, mode)
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._queue = None
        self._result = None
        self.msg_count = 1
        self.time_delta = time.time() + 30  # in seconds
        self._credentials = pika.PlainCredentials(self.USER, self.PASSWORD)
        self._parameters = pika.ConnectionParameters(host=self.HOST,
                                                     virtual_host=self.VIRTUAL_HOST,
                                                     ssl=self.SSL,
                                                     port=self.PORT,
                                                     credentials=self._credentials)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.file.close()

    def connect(self):
        """
        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.BlockingConnection
        """
        print('Connecting to',  self.HOST)
        return pika.BlockingConnection(self._parameters)

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        # print('Creating a new channel')  #debug
        return self._connection.channel()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue: The name of the queue to declare.
        """
        # print('Declaring queue', self.QUEUE)  #debug
        self._result = self._channel.queue_declare(queue=self.QUEUE,
                                    durable=self.DURABLE,
                                    arguments=self.ARGUMENTS)
        self._queue = self._result.method.queue

    def consumer_callback(self, ch, method, properties, body):
        """
        This function receives ch, method, properties,
        and body data from Rabbit MQ and prints the body to a file.
        It uses the number of seconds to determine when to close.
        """
        # logger.debug('Seconds remaining before closing queue: {}'.format(self.time_delta - time.time()))  # debug
        # logger.debug('Message Number: {}'.format(str(self.msg_count)))
        # self.msg_count += 1

        if self.time_delta - time.time() > 0:
            # print(self._result.method.message_count)  # debug code
            logger.debug("Message Num: [{}] Received: [{}] Seconds Remaining [{:06.3f}]".format(str(self.msg_count).zfill(3), str(len(body)).zfill(3), (self.time_delta - time.time())))
            self.file.write(body.decode('utf-8') + '\n')
            # f.write(body.decode('utf-8') + '\n')
            # print(body.decode('utf-8') + '\n')
            self._channel.basic_ack(method.delivery_tag)
            # self._result.method.message_count -= 1  #debug code
            # logger.debug('Message Number: {}'.format(str(self.msg_count)))
            self.msg_count += 1
        else:
            self.stop()

    def consumer_callback_num_msg_stdout(self, ch, method, properties, body):
        """
        This function receives ch, method, properties,
        and body data from Rabbit MQ and prints the body to stdout.
        It uses the number of messages in the queue initially
        to determine when to close.
        """
        if self._result.method.message_count > 0:
            print(" [x] Received: [{}]".format(len(body)))
            print(self._result.method.message_count)
            # f.write(body.decode('utf-8') + '\n')
            print(body.decode('utf-8') + '\n')
            self._channel.basic_ack(method.delivery_tag)
            self._result.method.message_count -= 1
        else:
            self.stop()

    def consumer_callback_time_stdout(self, ch, method, properties, body):
        """
        This function receives ch, method, properties,
        and body data from Rabbit MQ and prints the body to stdout.
        It uses the number of seconds to determine when to close.
        """
        print('Seconds remaining before closing queue:',
              self.time_delta - time.time())  # debug code
        if self.time_delta - time.time() > 0:
            # print(self._result.method.message_count)  # debug code
            print(" [x] Received: [{}]".format(len(body)))
            # print(body.decode('utf-8') + '\n')
            self._channel.basic_ack(method.delivery_tag)
            # self._result.method.message_count -= 1  #debug code
        else:
            self.stop()


    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            print('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag)

    def run(self):
        self._connection = self.connect()
        self._channel = self.open_channel()
        self.setup_queue()
        consumer_tag = self._channel.basic_consume(self.consumer_callback,
                                                   queue=self._queue)
        print('Consumer Tag: {}'.format(consumer_tag))
        print('Number of messages currently in queue:',
              self._result.method.message_count)
        self._channel.start_consuming()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closw the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        # print('Stopping')  #debug
        self._closing = True
        self.stop_consuming()
        # print('Consumer Stopped') #debug
        self.close_connection()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        # print('Closing connection') #debug
        self._connection.close()

def main():
    # print('Start Main')  #debug
    sensor_data_consumer = RabbitmqConsumer(filepath, 'w')
    # sensor_data_consumer = RabbitmqConsumer(filepath, 'a')
    try:
        sensor_data_consumer.run()
    except KeyboardInterrupt:
        sensor_data_consumer.stop()
    finally:
        sensor_data_consumer.stop()
    # print('End Main')  #debug

if __name__ == '__main__':
    main()
    num_lines = sum(1 for line in open(filepath) if line.rstrip())
    logger.debug('Number of messages read from queue: {}\n'.format(num_lines))
