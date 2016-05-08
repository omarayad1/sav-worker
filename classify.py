import pika
import time
import os
import json
import sys
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import *
from sqlalchemy.orm import create_session,sessionmaker
from sqlalchemy.ext.declarative import declarative_base

##### config
caffe_root = os.environ['CAFFE_HOME']+'/'
postgres_url = os.environ['POSTGRES_URL']
highProb = 5

sys.path.insert(0, caffe_root + 'python')
import caffe

Base = declarative_base()
engine = create_engine(postgres_url)
metadata = MetaData(bind=engine)

class Tasks(Base):
    __table__ = Table('tasks', metadata, autoload=True)

class Users(Base):
    __table__ = Table('users', metadata, autoload=True)

session = sessionmaker(bind=engine)()

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='classify', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def classifyImage(imagePath):
    plt.rcParams['figure.figsize'] = (10, 10)
    plt.rcParams['image.interpolation'] = 'nearest'
    plt.rcParams['image.cmap'] = 'gray'

    model_def = caffe_root + 'models/bvlc_alexnet/deploy.prototxt'
    model_weights = caffe_root + 'models/bvlc_alexnet/bvlc_alexnet.caffemodel'
    net = caffe.Net(model_def,
                    model_weights,
                    caffe.TEST)

    mu = np.load(caffe_root + 'python/caffe/imagenet/ilsvrc_2012_mean.npy')
    mu = mu.mean(1).mean(1)
    transformer = caffe.io.Transformer({'data': net.blobs['data'].data.shape})

    transformer.set_transpose('data', (2,0,1))
    transformer.set_mean('data', mu)
    transformer.set_raw_scale('data', 255)
    transformer.set_channel_swap('data', (2,1,0))

    net.blobs['data'].reshape(50,3,227, 227)
    if imagePath is not None:
        image = caffe.io.load_image(imagePath)
        transformed_image = transformer.preprocess('data', image)
        plt.imshow(image)

        net.blobs['data'].data[...] = transformed_image
        output = net.forward()
        output_prob = output['prob'][0]
        labels_file = caffe_root + 'data/ilsvrc12/synset_words.txt'
        labels = np.loadtxt(labels_file, str, delimiter='\t')
        top_inds = output_prob.argsort()[::-1][:highProb]
        top_labels = labels [top_inds];
        cleanLabels = []
        for index in top_labels:
            cleanLabels.append(index[10:])
        print cleanLabels
        return cleanLabels

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    task = session.query(Tasks).filter_by(id=int(body)).first()
    task.status = "recieved by classifier, running classifier"
    session.commit()
    task = session.query(Tasks).filter_by(id=int(body)).first()
    if task.type == 'image':
        result = classifyImage(json.loads(task.__dict__['file'])[0])
    else:

    task.status = "finished classifing, sending to fuser"
    task.dataClassify = str(result)
    session.commit()
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue='classify')

channel.start_consuming()
