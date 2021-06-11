#!/bin/bash
users=("user1.comcast.com" "user2.comcast.com" "user3.att.com" "user4.att.com")

actions=("http://localhost:5000/" "http://localhost:5000/join_guild?id=" "http://localhost:5000/purchase_a_sword?id=" "http://localhost:5000/purchase_health?id=")

user=${users[$((RANDOM % ${#users[@]}))]}

action=${actions[$((RANDOM % ${#actions[@]}))]}

#docker-compose exec mids ab -n 1 -H "Host:$user" $action

if [ $action == "http://localhost:5000/purchase_a_sword?id=" ]; then
    id=$((RANDOM % 4))
    docker-compose exec mids ab -n 1 -H "Host: $user" $action$id
elif [ $action == "http://localhost:5000/purchase_health?id=" ]; then
    id=$((RANDOM % 3))
    docker-compose exec mids ab -n 1 -H "Host: $user" $action$id
elif [ $action == "http://localhost:5000/join_guild?id=" ]; then
    id=$((RANDOM % 3))
    docker-compose exec mids ab -n 1 -H "Host: $user" $action$id
else
    docker-compose exec mids ab -n 1 -H "Host: $user" $action
fi

