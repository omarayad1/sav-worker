import pika
import time
import os
from sqlalchemy import *
from sqlalchemy.orm import create_session,sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#Create and engine and get the metadata
Base = declarative_base()
engine = create_engine(os.environ['POSTGRES_URL'])
metadata = MetaData(bind=engine)

#Reflect each database table we need to use, using metadata
class Tasks(Base):
    __table__ = Table('tasks', metadata, autoload=True)

class Users(Base):
    __table__ = Table('users', metadata, autoload=True)

#Create a session to use the tables
session = sessionmaker(bind=engine)()

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='classify', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def extractKf(filenames):
    return ["file1.jpg", "file2.jpg", "file3.jpg"]

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    task = session.query(Tasks).filter_by(id=int(body)).first()
    task.status = "recieved by classifier, running classifier"
    session.commit()
    task = session.query(Tasks).filter_by(id=int(body)).first()
    result = extractKf(task.__dict__['file'])
    task.status = "finished classifing, sending to fuser"
    task.dataClassify = str(result)
    session.commit()
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue='classify')

channel.start_consuming()
