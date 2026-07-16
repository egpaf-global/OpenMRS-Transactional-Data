# Databricks notebook source
from src.auth import login
from config.credentials import EMAIL, PASSWORD

token = login(EMAIL, PASSWORD)

print(token)
