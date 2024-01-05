#!/bin/bash
gnome-terminal -- docker-compose up --force-recreate --build
gnome-terminal -- docker-compose -f ./user/docker-compose.yml up --force-recreate --build
gnome-terminal -- docker-compose -f ./auth/docker-compose.yml up --force-recreate --build
gnome-terminal -- docker-compose -f ./geo/docker-compose.yml up --force-recreate --build
gnome-terminal -- docker-compose -f ./notify/docker-compose.yml up --force-recreate --build