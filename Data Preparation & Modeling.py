# Databricks notebook source
import pandas as pd

# Load Unity Catalog table -> Spark -> pandas
pdf = spark.table("workspace.default.customer_shopping_behavior").toPandas()

pdf.head()

# COMMAND ----------

pdf.info()

# COMMAND ----------

pdf.describe()

# COMMAND ----------

pdf.isnull().sum()

# COMMAND ----------

 pdf['Review Rating'] = pdf.groupby( 'Category')['Review Rating']. transform(lambda x: x.fillna(x.median()))

# COMMAND ----------

pdf.isnull().sum()

# COMMAND ----------

pdf.columns = pdf.columns.str.lower()
pdf.columns = pdf.columns.str.replace(' ','_')

# COMMAND ----------

pdf.columns

# COMMAND ----------

pdf=pdf.rename(columns={'purchase_amount_(usd)':'purchase_amount'})


# COMMAND ----------

labels = ['Young Adult', 'Adult', 'Middle-aged' , 'Senior']
pdf['age_group'] = pd.qcut(pdf['age'], q=4, labels=labels)
pdf[['age','age_group']].head(10)

# COMMAND ----------

frequency_mapping={
'Fortnightly': 14,
'Weekly': 7,
'Monthly': 39,
'Quarterly': 90,
'Bi-Week1y': 14,
'Annually': 365,
'Every 3 Months': 90
}
pdf['purchase_frequency_days']=pdf['frequency_of_purchases'].map(frequency_mapping)

# COMMAND ----------

pdf[['purchase_frequency_days','frequency_of_purchases']].head(10)

# COMMAND ----------

pdf[['discount_applied','promo_code_used']].head(10)

# COMMAND ----------

(pdf['discount_applied']==pdf['promo_code_used']).all()

# COMMAND ----------

pdf=pdf.drop('promo_code_used',axis=1)

# COMMAND ----------

pdf.columns

# COMMAND ----------

