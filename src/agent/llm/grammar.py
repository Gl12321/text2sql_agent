import re


class SQLGrammarBuilder:
    @staticmethod
    def build(schema_mapping: dict[str, dict[str, list[str]]]) -> str:
        if not schema_mapping:
            return ""

        table_specific_rules = []
        qualified_table_names = []
        qualified_col_pairs = []
        all_unique_cols = set()

        for schema_name, tables in schema_mapping.items():
            for table_name, columns in tables.items():
                clean_id = re.sub(r'[^a-zA-Z0-9]', '', f"{schema_name}{table_name}")
                rule_name = f"{clean_id}Col"

                col_list_str = " | ".join([f'"{col}"' for col in columns])
                table_specific_rules.append(f'{rule_name} ::= {col_list_str}')

                table_path = f'("{schema_name}" "." "{table_name}")'
                qualified_table_names.append(table_path)

                qualified_col_pairs.append(f'({table_path} "." {rule_name})')

                all_unique_cols.update(columns)

        any_col_rule = " | ".join([f'"{c}"' for c in sorted(list(all_unique_cols))])
        tablename_rule = " | ".join(qualified_table_names)
        qualified_col_rule = " | ".join(qualified_col_pairs)

        lines = [
            "root ::= query",
            'query ::= select ";"?',

            'select ::= "SELECT " ( "DISTINCT " )? collist " FROM " tablename (joinClause)* (whereClause)? (groupClause)? (orderClause)? (limitClause)?',

            'collist ::= colitem (", " colitem)*',
            'colitem ::= (qualifiedCol | "*" | function)',

            'joinClause ::= " JOIN " tablename " ON " expr',
            'whereClause ::= " WHERE " expr',
            'groupClause ::= " GROUP BY " collist',
            'orderClause ::= " ORDER BY " collist ( " ASC" | " DESC" )?',
            'limitClause ::= " LIMIT " [0-9]+',

            f'tablename ::= {tablename_rule}',
            f'qualifiedCol ::= {qualified_col_rule}',
            f'anyCol ::= {any_col_rule}',

            "\n".join(table_specific_rules),

            'expr ::= term ( " " binop " " term )*',
            'term ::= (qualifiedCol | anyCol | stringlit | number | function | "(" expr ")")',
            'binop ::= "=" | "!=" | "<" | ">" | "<=" | ">=" | " AND " | " OR " | " LIKE " | " ILIKE " | " IN "',
            'function ::= ("COUNT" | "SUM" | "AVG" | "MAX" | "MIN") "(" (anyCol | qualifiedCol | "*") ")"',

            'stringlit ::= "\'" [^\']* "\'"',
            'number ::= [0-9]+ ("." [0-9]+)?'
        ]

        return "\n".join(lines)