import re

class SQLGrammarBuilder:
    @staticmethod
    def build(schema_mapping: dict[str, list[str]]) -> str:
        if not schema_mapping:
            return ""

        table_specific_rules = []
        qualified_pairs = []
        all_unique_cols = set()
        table_names = list(schema_mapping.keys())

        for table, columns in schema_mapping.items():
            clean_name = re.sub(r'[^a-zA-Z0-9]', '', table)
            rule_name = f"{clean_name}Col" 
            col_list_str = " | ".join([f'"{col}"' for col in columns])
            table_specific_rules.append(f'{rule_name} ::= {col_list_str}')
            
            # Строгая привязка: "Table"."ColOfThisTable"
            qualified_pairs.append(f'("{table}" "." {rule_name})')
            all_unique_cols.update(columns)

        any_col_rule = " | ".join([f'"{c}"' for c in sorted(list(all_unique_cols))])
        tablename_rule = " | ".join([f'"{t}"' for t in table_names])
        qualified_col_rule = " | ".join(qualified_pairs)

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
