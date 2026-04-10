embedding_langchain_models = {
    "hugging_face": "HuggingFaceEmbeddings",
}

ai_langchain_models = {
    "open_ai_azure": "AzureChatOpenAI",
    "open_ai": "ChatOpenAI",
    "ai_dial": "AzureChatOpenAI"
}

# Entity type mapping for resource-scoped MCP endpoints
ENTITY_TYPE_MAP = {
    "toolkit": "toolkit",
    "application": "application",
    "agent": "application",  # Alias for application
    "pipeline": "application",  # Pipelines use same structure as applications
}
