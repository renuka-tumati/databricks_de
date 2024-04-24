# Databricks notebook source
pip install spacy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Install spaCy (if not already installed)
#!pip install spacy

# Download the English language model for spaCy (if not already downloaded)
#!python -m spacy download en_core_web_sm

# Import spaCy
import spacy

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")

# Process your unstructured text
doc = nlp("""Document 3:
Title: Photography Tips for Beginners
Content: Photography is an art form that requires creativity and technical skill. For beginners, learning about composition, lighting, and camera settings is essential. Practice and experimentation are key to improving your photography skills.
readers count for document 3 is 2000, Average Time spent reading the document is 100 seconds
Date of document creation 4/22/2024 
expiry date of promotion on document is 4/30/2025""")

# Extract named entities
print(doc.ents)
print(type(doc))
# print(doc.vector)
for ent in doc.ents:
    print("--------------------")
    print(ent.label_)
    print(ent.text)
    if ent.label_ == "CARDINAL":  # Filter for numerical values
        print(f"Numerical value: {ent.text}")
    if ent.label_ == "DATE":  # Filter for numerical values
        print(f"Date value: {ent.text}")


# COMMAND ----------

import re

# Sample unstructured data containing numerical values
unstructured_data1 = """
Customer ID: 12345
Transaction amount: $500.25
Product ID: XYZ-6789
Quantity: 10
"""

unstructured_data2 = """Document 3:
Title: Photography Tips for Beginners
Content: Photography is an art form that requires creativity and technical skill. For beginners, learning about composition, lighting, and camera settings is essential. Practice and experimentation are key to improving your photography skills.
reader count for document 3 is 2000, Average Time spent reading the document is 100 seconds"""

# Define regular expression patterns to extract numerical values
number_patterns = [
    r'\b\d+\b',         # Match integers
    r'\b\d+\.\d+\b',    # Match floating point numbers
    r'\$\d+\.\d+',      # Match dollar amounts
]

# Extract numerical values using regular expressions
numerical_values1 = []
numerical_values2 = []
for pattern in number_patterns:
    numerical_values1.extend(re.findall(pattern, unstructured_data2))

# Print the extracted numerical values
print("Extracted numerical values:", numerical_values1)

for pattern in number_patterns:
    numerical_values2.extend(re.findall(pattern, unstructured_data1))

# Print the extracted numerical values
print("Extracted numerical values:", numerical_values2)

