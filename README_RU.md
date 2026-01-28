**SQL Agent**

*Краткое описание*: AI-агент для генерации SQL-запросов к реляционным базам данных с механизмом самокоррекции
    и RAG-системой на базе метаданных, для работы локально на CPU
*Стек*: Python 3.12 | FastAPI  | LangGraph | Llama-cpp-python (GGUF/CPU) | SQLAlchemy | PostgreSQL | ChromaDB | Sentence-Transformers | Streamlit | Docker |
*Архитектура*: DB(Postgres) -> RAG(Chroma) -> LLM(Llama-3-8B-Instruct tunned_for_sql) -> Agent(LangGraph) -> API(FastAPI) -> Frontend(Streamlit)

**Key Modules & Technical Features:**

*Database Layer* (src/database)
    Реализованный уровень базы данных обеспечивает полный цикл обработки метаданных:
    от автоматизированной миграции в SchemaMigration (migration.py) из SQLite в PostgreSQL
    с восстановлением реляционной целостности до инспекции схем через асинхронный клиент PostgresClient
    (postgres_client.py) на базе SQLAlchemy 2.0 с использованием паттернов Singleton и Connection Pooling
    для эффективного управления конкурентными запросами. Модуль SchemaParser (schema_parser.py)
    реализует динамическую генерацию DDL-описания таблиц и карты связей через run_sync инспекцию
    для подачи в промпт, а также текстовое представление схемы для подачи в модули Embedding и Reranker. 

*RAG & Schema Discovery* (src/rag)
    Реализован двухэтапный конвейер извлечения контекста (Retrieval-Augmented Generation),
    оптимизированный для работы с многосхемными БД. На первом этапе TableRetriever (retriver.py)
    выполняет семантический поиск по векторному хранилищу ChromaDB с использованием эмбеддингов
    TableEmbedder (embedder.py), фильтруя таблицы по schema_id для соблюдения изоляции данных.
    Полученные результаты обрабатываются TableReranker (reranker.py), который на основе
    прямой оценки пары «вопрос-структура» отсеивает нерелевантные таблицы, оставляя в контексте
    только те сущности, которые необходимы для построения конкретного SQL-запроса.
    Модуль SchemaCataloger (cataloger.py) отвечает за индексацию, используя TableSerializer (serializer.py)
    для подготовки текстовых представлений схем. Это позволяет минимизировать шум в промпте и снизить риск
    галлюцинаций LLM при генерации сложных JOIN соединений.

*Agent Logic & Self-Correction* (src/agent)
    Логика работы системы реализована в виде графа состояний на базе LangGraph (graph.py),
    где ключевым этапом является совмещение автономного цикла исправления и жесткого синтаксического контроля.
    Через динамические GBNF-грамматики SQLGrammarBuilder (src/llm/grammar.py), которые ограничивают
    пространство вывода LLM только существующими в схеме таблицами и столбцами, исключается галлюцинация на этапе генерации.
    В случае возникновения ошибок выполнения, SQLExecutor (executor.py) задействует SQLCorrector (corrector.py)
    для активации Self-Correction Loop, анализируя трейсбэк PostgreSQL для итеративной правки кода.
    Условные переходы позволяют системе валидировать контекст и прерывать выполнение при нерелевантности данных,
    обеспечивая надежность и предсказуемость финального SQL-запроса. Инициализация моделей и инференс
    инкапсулированы в LLMWrapper с использованием шаблонов PromptManager.

**Implementation Details:** 
    Async/Sync Hybrid: Полностью асинхронный пайплайн обработки запросов, через SQLAlchemy 
    Local LLM Inference: Работа с GGUF-моделями через llama-cpp-python 
    Docker-native: Оркестрация через Docker Compose: изолированные контейнеры для PostgreSQL, Frontend и Core-приложения

**Deployment & Quick Start:**
*Требования к системе для стабильной работы* (Llama-3 8B Q4 + Embedder + Reranker):
    CPU: 8 ядер
    RAM: минимум 8 GB (лучше 16 GB+).
    Disk: ~15 GB свободного места (веса моделей + индексы).
    OS: Linux / macOS / Windows (Docker Desktop).
*Подготовка окружения:*
    Создайте файл .env в корне проекта:
        DB_USER=admin
        DB_PASSWORD=qwer1234
        DB_HOST=db (либо localhost для локального запуска)
        DB_PORT=5432
        
*Установка зависимостей:* pip install -r requirements.txt
    Загрузка весов: python download_models.py
    Создание образов: docker-compose up --build
*Запуск:*
    docker-compose up

UI (Streamlit): http://localhost:8501 — основной терминал.
API (Swagger): http://localhost:8000/docs — проверка эндпоинтов. 
