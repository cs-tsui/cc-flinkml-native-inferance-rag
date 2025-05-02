## Table of Contents
- [Confluent Cloud Flink Native AI Features - Implement RAG Workflow](#confluent-cloud-flink-native-ai-features---implement-rag-workflow)
  - [Prerequisites](#prerequisites)
    - [Confluent Cloud Setup](#confluent-cloud-setup)
    - [Vector Database Setup](#vector-database-setup)
  - [1. Real-time Embeddings Pipeline Setup](#1-real-time-embeddings-pipeline-setup)
  - [2. Fully Managed LLM Setup](#2-fully-managed-llm-setup)
  - [3. Put it all together: RAG Enhanced Prompts](#3-put-it-all-together-rag-enhanced-prompts)
  - [4. Cleanup](#4-cleanup)



## Confluent Cloud Flink Native AI Features - Implement RAG Workflow

In this tutorial, check out how we can perform a full RAG pipeline all within Confluent Cloud. What this means - with AI models hosted within Confluent Cloud, we can:

* Read from data sources and convert them into vectors with `fully managed embedding models` in Confluent Cloud
  * Source domain information (e.g. connector) -> Kafka Topic -> CC Native Embedding Model (Flink) -> Vector database
* perform vector search directly within FlinkSQL with `VECTOR_SEARCH` function
* enhance the user prompts with the vector search results and send it to `fully managed LLM models` for more accurate results 


### Prerequisites

#### Confluent Cloud Setup

Login to confluent cloud via the CLI. Make sure you're on the recent version of the CLI tool

```shell
confluent login
```

Use an existing cluster or create a new basic cluster for this tutorial.

Then, create a topic `product_information`

Example CLI commands to creat the topic 

```shell
confluent kafka topic create product_information
```

Create a datagen connector using the `SHOES` quickstart schema. Use the CLI or UI to create the connector instance

Example of using CLI:
Replace the two varibles with your API Key and Secret. Create one through CLI command (`confluent api-key create`) or follow the instructions in the UI.

```shell
export CONNECTOR_API_KEY='<REPLACE_WITH_API_KEY>'
export CONNECTOR_API_SECRET='<REPLACE_WITH_API_SECRET>'


cat <<EOF > product_dg.json
{
  "config": {
    "connector.class": "DatagenSource",
    "name": "product_information_dg",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "$CONNECTOR_API_KEY",
    "kafka.api.secret": "$CONNECTOR_API_SECRET",
    "kafka.topic": "product_information",
    "schema.context.name": "default",
    "output.data.format": "AVRO",
    "quickstart": "SHOES",
    "max.interval": "3000",
    "tasks.max": "1"
  }
}
EOF
```

Create the source connector instance
```
confluent connect cluster create --config-file product_dg.json
```

Check to see the connector is running
```
confluent connect cluster list
```

#### Vector Database Setup

TODO: add instructions here to the elastic setup

Create an index to store embeddings which we will generate as part of this tutorial.


```shell
export ELASTICSEARCH_ENDPOINT='<ES_ENDPOINT>'
export ELASTIC_API_KEY='<ES_APIK_KEY>'

confluent flink connection create cst-elastic-connection \
  --cloud AWS \
  --region us-east-1 \
  --type elastic \
  --endpoint ${ELASTICSEARCH_ENDPOINT} \
  --api-key ${ELASTIC_API_KEY}
```

### 1 Real-time Embeddings Pipeline Setup

The rest of the demo can be completed fully within Flink Workspaces

```sql
-- setup managed embedding model
CREATE MODEL `managed_model_embedding`
INPUT (text STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider' = 'confluent',
  'task' = 'EMBEDDING',
  'confluent.model'='BAAI/bge-large-en-v1.5'
);
```


```sql
-- TEST: datagen should be sending data into this topic
select * from product_information limit 5;


-- create a table augmented with text string that combines all the useful columns of information

create table product_content
as
select key,
	id,
	brand,
	name,
	sale_price,
	rating,
	concat_ws(' ',
    'product id: '      || id,
    ', brand: '         || brand,
    ', name: '          || name,
    ', sale_price: '    || cast(sale_price as string),
    ', rating number: ' || cast(rating as string)
	) as content
from product_information;


-- now we're gonna turn this product content column into embeddings
create table product_embeddings
as 
select * from product_content,
lateral table (ml_predict('managed_model_embedding', content));


-- The resulting table have an `embedding` column that's generated from the  `content` column
select * from product_embeddings limit 5;


-- Test the vector search 
SELECT product_embeddings.content, 
        product_embeddings.embedding as original_embedding, 
        search_results FROM product_embeddings, 
  LATERAL TABLE(VECTOR_SEARCH(elastic_external, 1, DESCRIPTOR(embedding), embedding));
```

### 2 Fully Managed LLM Setup
Create source topic that is used for user prompt inputs

```sql
-- create user prompt input topic
CREATE TABLE text_stream_input (
  id BIGINT,
  prompt STRING
);
```


Create fully managed LLM model

```sql
-- setup managed llm model
CREATE MODEL `managed_model_llm`
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'microsoft/Phi-3.5-mini-instruct'
);

DESCRIBE MODEL `managed_model_llm`;

-- test the model, run select in a cell 
SELECT id, prompt, response
FROM text_stream_input, LATERAL TABLE(ML_PREDICT('`managed_model_llm`', prompt));

--- in a different workspace cell
INSERT INTO text_stream_input
  VALUES
    (1, 'The mitochondria is the powerhouse of the cell'),
    (2, 'Tell me a bit about Tiananmen Square'),
    (3, 'How many rs are there in strawberry');
```

The result from the `select` above is the response from the LLM with no RAG enhancing the prompt and context. Feel free to prompt it with different questions to see how the model respond by default.

### 3. Put it all together: RAG Enhanced Prompts

```sql
WITH 
  -- user prompt sent into "text_stream_input" gets an embedding generated with it
  prompt_embedding as ( 
    SELECT id,
      prompt,
      embedding
    FROM text_stream_input,
      lateral table (ML_PREDICT('managed_model_embedding', prompt))
  ),

  -- prompt_embedding.embedding, select the top 1 search results
  prompt_vsearch_results as ( 
    SELECT * 
    FROM prompt_embedding,
      LATERAL TABLE(
        VECTOR_SEARCH(
          elastic_external,1,DESCRIPTOR(embedding), prompt_embedding.embedding
        )
      )
  ),

  -- augement the user prompt with our vector search results for additional context.
  augmented_prompt as (
    select id,
      prompt_vsearch_results.prompt,
      vsearch_content,
      concat(
        'You are a helpful assistant.',
        'Using the following context: "',
        vsearch_content,
        '" to answer this user query: "',
        prompt_vsearch_results.prompt,
        '"'
      ) as enhanced_prompt
    from prompt_vsearch_results
      CROSS JOIN UNNEST(search_results) AS T(vsearch_embedding, vsearch_content)
  )

-- leverage the enhanced prompt and make an inferenace call tot he fully mabnaged LLM model
SELECT id,
  enhanced_prompt,
  response
FROM augmented_prompt,
  LATERAL TABLE(
    ML_PREDICT('`managed_model_llm`', enhanced_prompt)
  );

-- this is a query where we're sending our prompt directly to the LLM with no RAG enhancement
-- on a separate cell, if you stopped this query from running before
SELECT id, prompt, response
FROM text_stream_input, LATERAL TABLE(ML_PREDICT('`managed_model_llm`', prompt));

-- Ask a question that is specific to our domain (i.e. about product specifics that the LLM doesn't have knowledge about)
INSERT INTO text_stream_input
  VALUES
    (4,"whats the price of of Small Infant Female gloves?")

-- compare the result from the direct LLM call with no RAG. What are the responses that you get? 
```

### 4. Cleanup

Don't forget to clean up your Confluent Cloud resources after this tutorial
