    **SQL Agent**

    *Brief Description*: AI agent for generating SQL queries for relational databases with a self-correction mechanism 
                       and a metadata-based RAG system, designed for local operation on CPU.
    *Stack*: Python 3.12 | FastAPI | LangGraph | Llama-cpp-python (GGUF/CPU) | SQLAlchemy | PostgreSQL | ChromaDB | Sentence-Transformers | Streamlit | Docker |
    
    *Architecture:* DB(Postgres) -> RAG(Chroma) -> LLM(Llama-3-8B-Instruct tuned_for_sql) -> Agent(LangGraph) -> API(FastAPI) -> Frontend(Streamlit)

    **Key Modules & Technical Features:**

    *Database Layer* (src/database)
        The implemented database layer provides a full metadata processing cycle: 
        from automated migration in SchemaMigration (migration.py) from SQLite to PostgreSQL 
        with restoration of relational integrity, to schema inspection via the asynchronous PostgresClient 
        (postgres_client.py) based on SQLAlchemy 2.0 using Singleton and Connection Pooling patterns 
        for efficient concurrent request management. The SchemaParser module (schema_parser.py) 
        implements dynamic generation of DDL table descriptions and relationship maps via run_sync inspection 
        for prompt feeding, as well as text schema representations for the Embedding and Reranker modules.

    *RAG & Schema Discovery* (src/rag)
        A two-stage context extraction pipeline (Retrieval-Augmented Generation) has been implemented, 
        optimized for working with multi-schema databases. At the first stage, TableRetriever (retriver.py) 
        performs semantic search across the ChromaDB vector store using TableEmbedder (embedder.py) 
        embeddings, filtering tables by schema_id to maintain data isolation. 
        The obtained results are processed by TableReranker (reranker.py), which, based on 
        a direct evaluation of the "question-structure" pair, filters out irrelevant tables, leaving in context 
        only those entities necessary for building a specific SQL query. 
        The SchemaCataloger module (cataloger.py) is responsible for indexing, using TableSerializer (serializer.py) 
        to prepare text schema representations. This minimizes noise in the prompt and reduces the risk 
        of LLM hallucinations when generating complex JOIN connections.

    *Agent Logic & Self-Correction* (src/agent)
        The system's operation logic is implemented as a state graph based on LangGraph (graph.py), 
        where the key stage is combining an autonomous correction cycle with strict syntax control. 
        Through dynamic GBNF grammars in SQLGrammarBuilder (src/llm/grammar.py), which restrict 
        the LLM output space only to tables and columns existing in the schema, hallucinations are eliminated during the generation stage. 
        In case of execution errors, SQLExecutor (executor.py) engages SQLCorrector (corrector.py) 
        to activate the Self-Correction Loop, analyzing the PostgreSQL traceback for iterative code adjustment. 
        Conditional transitions allow the system to validate context and abort execution if data is irrelevant, 
        ensuring the reliability and predictability of the final SQL query. Model initialization and inference 
        are encapsulated in LLMWrapper using PromptManager templates.

    **Implementation Details:** 
        Async/Sync Hybrid: Fully asynchronous request processing pipeline via SQLAlchemy. 
        Local LLM Inference: Working with GGUF models via llama-cpp-python. 
        Docker-native: Orchestration via Docker Compose: isolated containers for PostgreSQL, Frontend, and the Core application.

    **Deployment & Quick Start**
    *System requirements for stable operation* (Llama-3 8B Q4 + Embedder + Reranker):
        CPU: 8 cores
        RAM: minimum 8 GB (16 GB+ recommended).
        Disk: ~15 GB free space (model weights + indices).
        OS: Linux / macOS / Windows (Docker Desktop).

    *Environment preparation:*
        Create a .env file in the project root:
            DB_USER=admin
            DB_PASSWORD=qwer1234
            DB_HOST=db (or localhost for local run)
            DB_PORT=5432
        Install dependencies: pip install -r requirements.txt
        Download weights: python download_models.py
        Build images: docker-compose up --build
    *Run:*
        docker-compose up

        UI (Streamlit): http://localhost:8501 — main terminal.
        API (Swagger): http://localhost:8000/docs — endpoint verification.