# Chapter 14 reference: Instrumented retrieval and generation.
# Inject compatible real retriever and model adapters.
# Live Databricks execution has NOT been verified.

import mlflow

@mlflow.trace(name="retrieve_context")
def retrieve_context(query):
    return retriever(query)  # Inject a real, authorized retriever.

@mlflow.trace(name="llm_generation")
def generate_answer(query, context):
    return chat_client(query, context)  # Tested serving adapter.

@mlflow.trace(name="full_rag_execution")
def answer_question(query):
    context = retrieve_context(query)
    return generate_answer(query, context)
