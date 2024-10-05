@echo off
REM Create the main folders
mkdir data
mkdir analysis
mkdir pipeline



REM Create the files in the Data folder
cd data
mkdir raw
mkdir transformed
mkdir input

REM Go back to the root directory
cd ..

REM Create the Jupyter notebook in the Analysis folder
cd Analysis
echo {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 2} > exploration.ipynb

REM Go back to the root directory
cd ..

REM Create the Python scripts in the Pipeline folder
cd pipeline
echo # This is extract.py > extract.py
echo # This is load.py > load.py
echo # This is transform.py > transform.py

REM Go back to the root directory
cd ..




REM Install libraries from requirements.txt if it exists
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo # List your required libraries here > requirements.txt
)

echo Folders and files created successfully.
