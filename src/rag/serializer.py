from src.core.logger import setup_logger

logger = setup_logger("serializer")

class TableSerializer:
    def __call__(self, table_metadata: dict) -> str:
        cols = []
        for c in table_metadata['columns']:
            base = f"{c['name']} {str(c['type'])}"
            if c.get('comment'):
                base += f" ({c['comment']})"
            cols.append(base)

        doc = [
            f"schema: {table_metadata['schema_name']}",
            # ИСПРАВЛЕНО: 'name' -> 'table_name'
            f"Table: {table_metadata['table_name']}",
            f"Columns: {', '.join(cols)}"
        ]

        if table_metadata.get('primary_keys'):
            doc.append(f"Primary keys: {', '.join(table_metadata['primary_keys'])}")

        if table_metadata.get('foreign_keys'):
            fks = []
            for fk in table_metadata['foreign_keys']:
                constrained = ', '.join(fk['constrained_columns'])
                referred = ', '.join(fk['referred_columns'])
                fks.append(f"{constrained} -> {fk['referred_table']}.{referred}")
            doc.append(f"Foreign keys: {', '.join(fks)}")

        logger.info(f"Serialized table {table_metadata['table_name']}")
        return '\n'.join(doc)
