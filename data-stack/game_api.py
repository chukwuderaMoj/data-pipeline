#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


swords = [
        {'id': 0,
         'style': 'Iron Sword',
         'Damage': 15,
         'Speed': 2,
         'Price': 70},
        {'id': 1,
         'style': 'Titanium Sword',
         'Damage': 15,
         'Speed': 4,
         'Price': 100},
        {'id': 2,
         'style': 'Axe',
         'Damage': 20,
         'Speed': 1,
         'Price': 50},
        {'id': 3,
         'style': 'Knife',
         'Damage': 5,
         'Speed': 5,
         'Price': 20}
        ]

hearts = [
        {'id': 0,
         'size': 'Small Heart',
         'health': 5,
         'Price': 10},
        {'id': 1,
         'size': 'Medium Heart',
         'health': 10,
         'Price': 20},
        {'id': 2,
         'size': 'Big Heart',
         'health': 20,
         'Price': 30},
        ]

guilds = [
        {'id': 0,
         'name': 'Horde'},
        {'id': 1,
         'name': 'Gods'},
        {'id': 2,
         'name': 'Gnomes'}
        ]

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

@app.route("/purchase_a_sword", methods=['GET'])
def purchase_a_sword():
    if 'id' in request.args: 
        if request.args['id'].isnumeric():
            id = int(request.args['id'])
        else:
            purchase_a_sword_event = {'event_type': 'purchase_a_sword',
                                      'style': 'NULL',
                                      'id': 'NULL',
                                      'Damage': 'NULL',
                                      'Speed': 'NULL',
                                      'Price': 'NULL',
                                      'success': False}
            log_to_kafka('events', purchase_a_sword_event)
            return "Error: Sword id provided is not an integer. Purchase failed. Try again with a valid item id."
    else:
        purchase_a_sword_event = {'event_type': 'purchase_a_sword',
                                  'style': 'NULL',
                                  'id': 'NULL', 
                                  'Damage': 'NULL',
                                  'Speed': 'NULL',
                                  'Price': 'NULL',
                                  'success': False}
        log_to_kafka('events', purchase_a_sword_event)
        return "Error: No valid sword id provided. Purchase failed. Try again with a valid item id."

    for sword in swords:
        if sword['id'] == id:
            item = sword
            found = True
            break
        else:
            found = False

    if not found:
        purchase_a_sword_event = {'event_type': 'purchase_a_sword',
                                  'style': 'NOT FOUND',
                                  'id': id,
                                  'Damage': 'NULL',
                                  'Speed': 'NULL',
                                  'Price': 'NULL',
                                  'success': False}
        log_to_kafka('events', purchase_a_sword_event)
        return "Purchase failed! Id " + str(id) + " does not match any available item"
    else:
        purchase_a_sword_event = {'event_type': 'purchase_a_sword',
                                  'style': item['style'],
                                  'id': id,
                                  'Damage': item['Damage'],
                                  'Speed': item['Speed'],
                                  'Price': item['Price'],
                                  'success': True}
        log_to_kafka('events', purchase_a_sword_event)
        return "Purchase Successful! You purchased a bran new " + item['style']

@app.route("/purchase_health", methods=['GET'])
def purchase_some_health():
    if 'id' in request.args: 
        if request.args['id'].isnumeric():
            id = int(request.args['id'])
        else:
            purchase_health_event = {'event_type': 'purchase_health',
                                     'size': 'NULL',
                                     'id': 'NULL', 
                                     'health': 'NULL',
                                     'Price': 'NULL',
                                     'success': False}
            log_to_kafka('events', purchase_health_event)
            return "Error: Health id provided is not an integer. Purchase failed. Try again with a valid health id."
    else:
        purchase_health_event = {'event_type': 'purchase_health',
                                 'size': 'NULL',
                                 'id': 'NULL', 
                                 'health': 'NULL',
                                 'Price': 'NULL',
                                 'success': False}
        log_to_kafka('events', purchase_health_event)
        return "Error: No valid health id provided. Purchase failed. Try again with a valid health id."

    for heart in hearts:
        if heart['id'] == id:
            item = heart
            found = True
            break
        else:
            found = False

    if not found:
        purchase_health_event = {'event_type': 'purchase_health',
                                 'size': 'NOT FOUND',
                                 'id': id,
                                 'health': 'NULL',
                                 'Price': 'NULL',
                                 'success': False}
        log_to_kafka('events', purchase_health_event)
        return "Purchase failed! Id " + str(id) + " does not match any available health item"
    else:
        purchase_health_event = {'event_type': 'purchase_health',
                                 'size': heart['size'],
                                 'id': id,
                                 'health': heart['health'],
                                 'Price': heart['Price'],
                                 'success': True}
        log_to_kafka('events', purchase_health_event)
        return "Purchase Successful! You purchased a " + heart['size']


@app.route("/join_guild", methods=['GET'])
def join_a_guild():
    if 'id' in request.args: 
        if request.args['id'].isnumeric():
            id = int(request.args['id'])
        else:
            join_guild_event = {'event_type': 'join_guild',
                                'guild_name': 'NULL',
                                'id': 'NULL', 
                                'success': False}
            log_to_kafka('events', join_guild_event)
            return "Error: Guild id provided is not an integer. Joining guild failed. Try again with a valid guild id."
    else:
        join_guild_event = {'event_type': 'join_guild',
                            'guild_name': 'NULL',
                            'id': 'NULL', 
                            'success': False}
        log_to_kafka('events', join_guild_event)
        return "Error: No valid guild id provided. Joining Guild failed. Try again with a valid guild id."

    for guild in guilds:
        if guild['id'] == id:
            item = guild
            found = True
            break
        else:
            found = False

    if not found:
        join_guild_event = {'event_type': 'join_guild',
                            'guild_name': 'NOT FOUND',
                            'id': id,
                            'success': False}
        log_to_kafka('events', join_guild_event)
        return "Joining Guild failed! Id " + str(id) + " does not match any available Guild"
    else:
        join_guild_event = {'event_type': 'join_guild',
                            'guild_name': guild['name'],
                            'id': id,
                            'success': True}
        log_to_kafka('events', join_guild_event)
        return "You've successfully joined the " + guild['name'] + " Guild"

