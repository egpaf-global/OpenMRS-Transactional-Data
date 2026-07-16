# Databricks notebook source
from src.auth import login

token = login()

print(token)
