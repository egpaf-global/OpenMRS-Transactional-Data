# Databricks notebook source
from src.auth import login

token = login("admin@openmrsg360.org", "g360!MalawiD4ta@2006")

print(token)