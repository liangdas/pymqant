# -*- coding: utf-8 -*-
'''
Created on 2017/3/16.
@author: love
'''
import pika

class ExampleConsumer(object):

    def __init__(self, amqp_info):
        """
        {
            Uri          string
            Exchange     string
            ExchangeType string
            Queue        string
            BindingKey   string //
            ConsumerTag  string //消费者TAG
        }
        :param amqp_info:
        :return:
        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._url = amqp_info["Uri"]
        self.Exchange = amqp_info["Exchange"]
        self.ExchangeType = amqp_info["ExchangeType"]
        self.Queue = amqp_info["Queue"]
        self.BindingKey = amqp_info["BindingKey"]
        self._consumer_tag = amqp_info["ConsumerTag"]

    def connect(self):
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self._on_connection_open,
                                     stop_ioloop_on_close=False)

    def reconnect(self):
        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

    def _on_connection_open(self, unused_connection):
        self._connection.add_on_close_callback(self._on_connection_closed)
        self._open_channel()


    def _open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self.setup_exchange(self.Exchange)

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(self._on_exchange_declareok,
                                       exchange_name,
                                       self.ExchangeType,
                                       durable=True)

    def _on_exchange_declareok(self, unused_frame):
        self.setup_queue(self.Queue)

    def setup_queue(self, queue_name):

        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(self.on_bindok, self.Queue,
                                 self.Exchange, self.BindingKey)
    def on_bindok(self, unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.Queue,self._consumer_tag)

    def _on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def close_connection(self):
        self._connection.close()

    def _on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.add_timeout(5, self.reconnect)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()

    def _close_channel(self):
        self._channel.close()

    def _on_cancelok(self, unused_frame):
        self._close_channel()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self._channel.basic_ack(basic_deliver.delivery_tag)

    def rabbitmq_run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def rabbitmq_stop(self):
        self._closing = True
        self._stop_consuming()
        self._connection.ioloop.stop()

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

