# Network Rail Train Delay Analytics

Network Rail owns, operates and develops Britain’s railway infrastructure. Network Rail's goal is to get people and goods where they need to be and to support Britain's economic prosperity. 

This project aims to provide analytics on Train Delay Attribution. 
Dataset: https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/

The dataset contains two types of files:
1. Historical data: 
2. Glossary: Dimensional attributes and description

Following file is a reference data for each column in the feed. 
https://sacuksprodnrdigital0001.blob.core.windows.net/historic-delay-attribution/Reference%20Files/Explanation-of-historic-attributed-delays.pdf

requirements.txt

Data Ingestion:
    - glossary ingestion from Excel
    - Data Ingestion Pipeline: 
        - Folders: 2018-19, 2019-20, 2020-21, 2021-22, 2022-23
        - File Pattern: All-delays-2021-22-P01.zip. Each folder ranges from P01-P13. Except 2022-2023= P01-P11
        - File Format: Compressed CSV for data files, xlsx for Glossary file
        - Example URL: https://sacuksprodnrdigital0001.blob.core.windows.net/historic-delay-attribution/2021-22/All-delays-2021-22-P01.zip

        