# Databricks notebook source
# DBTITLE 1,Installing mlflow to make sure we have the latest version.
# MAGIC %pip install mlflow

# COMMAND ----------

# DBTITLE 1,Using the roberta-large pipeline from Hugging Face
from transformers import pipeline
unmasker = pipeline('fill-mask', model='roberta-large')

# COMMAND ----------

# DBTITLE 1,Helper function to pull the text from the fill-mask response
def get_mask_fill_text(input_text):
  item = unmasker(input_text)
  # return {x['token_str'].strip():x['score'] for x in item}
  return [x['token_str'].strip() for x in item]

# COMMAND ----------

# DBTITLE 1,Let's test out our mask-fill model on a test phrase.
emotions = get_mask_fill_text("I won a race today! This makes me feel <mask>.")
emotions

# COMMAND ----------

# DBTITLE 1,Specify the catalog and database for the training data.
# MAGIC %sql
# MAGIC use catalog mandy_baker_demo_catalog; -- REPLACE WITH YOUR OWN CATALOG HERE
# MAGIC use schema ai_demos; -- REPLACE WITH YOUR OWN SCHEMA HERE

# COMMAND ----------

# DBTITLE 1,Build the table for some training data.
# MAGIC %sql
# MAGIC create or replace table how_are_you_feeling (
# MAGIC     id long GENERATED ALWAYS AS IDENTITY,
# MAGIC     feeling VARCHAR(255)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Let's add in a couple of example phrases to test on.
# MAGIC %sql
# MAGIC INSERT INTO how_are_you_feeling (feeling) VALUES
# MAGIC     ('I won a race today!'),
# MAGIC     ('I dropped my favorite mug and it broke.'),
# MAGIC     ('I wish I had learned to play the piano.'),
# MAGIC     ('I am going to be the next Mozart!'),
# MAGIC     ('Piano is hard. I am not sure I can ever master it.');

# COMMAND ----------

# DBTITLE 1,Now let's create a list of prompts to evaluate.
prompts_list = ["I feel <mask>.",
                "This gives me the feeling of <mask>.",
                "This gives me the emotion of <mask>.",
                "This makes me feel the emotion of <mask>."]

# COMMAND ----------

# DBTITLE 1,Run experiments across our prompts to see how they compare.
import itertools
import pandas as pd
import mlflow

# convert our delta table to pandas for this experiment
model = [unmasker]  # include multiple options to experiment against
data = spark.table("mandy_baker_demo_catalog.ai_demos.how_are_you_feeling").toPandas()

for idx, prompt in enumerate(prompts_list):
    with mlflow.start_run(run_name="EVALUATE_PROMPT_"+str(idx)):
        mlflow.log_params({'model-type':'fill-mask', 'llm_model':'roberta-large', 'prompt':prompt})
        mlflow.log_text
        data['prompt']=prompt
        data['input']=data['feeling'].apply(lambda x:"{} {}".format(x, prompt))
        
        data['result']=data['input'].apply(lambda x:get_mask_fill_text(x))

		## Log the results as a table. This can be compared across runs in the artifact view (from the Experiments UI)
        mlflow.log_table(data, artifact_file="emotions_eval_results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can head to the Experiments page to take a look at how the prompts' outputs compare.
