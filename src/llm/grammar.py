class SQLGrammarBuilder:
    @staticmethod
    def build(tables: list[str], columns: list[str]) -> str:
        if not tables or not columns:
            raise ValueError("Tables and columns lists cannot be empty for grammar generation")

        t_rules = " | ".join([f'"{t}"' for t in tables])
        c_rules = " | ".join([f'"{c}"' for c in columns])

        grammar = f"""
        root        ::= sql_query
        sql_query   ::= select_stmt (";")?

        select_stmt ::= "SELECT " ( "DISTINCT " )? col_list " FROM " table_ref (join_clause)* (where_clause)? (group_clause)? (order_clause)? (limit_clause)?

        col_list    ::= col_item (", " col_item)*
        col_item    ::= (column_name | "*") ( " AS " [a-zA-Z_][a-zA-Z0-9_]* )? | func_call

        table_ref   ::= table_name ( " AS " [a-zA-Z_][a-zA-Z0-9_]* )?
        join_clause ::= " JOIN " table_ref " ON " expr
        where_clause::= " WHERE " expr
        group_clause::= " GROUP BY " col_list
        order_clause::= " ORDER BY " col_list (" ASC" | " DESC")?
        limit_clause::= " LIMIT " [0-9]+

        # Динамические идентификаторы из БД
        table_name  ::= {t_rules}
        column_name ::= {c_rules}

        # Выражения
        expr        ::= term ( " " binary_op " " term )*
        term        ::= column_name | table_name "." column_name | string_lit | number | func_call | "(" expr ")"
        binary_op   ::= "=" | "!=" | "<" | ">" | "<=" | ">=" | "AND" | "OR" | "LIKE" | "ILIKE" | "IN"

        func_call   ::= ("COUNT" | "SUM" | "AVG" | "MAX" | "MIN") "(" (column_name | "*") ")"

        # Литералы
        string_lit  ::= "'" [^']* "'"
        number      ::= [0-9]+ ("." [0-9]+)?
        """
        return grammar.strip()