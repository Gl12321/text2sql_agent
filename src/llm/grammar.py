class SQLGrammarBuilder:
    @staticmethod
    def build(tables: list[str], columns: list[str]) -> str:
        if not tables or not columns:
            raise ValueError("Tables and columns lists cannot be empty")


        t_rules = " | ".join([f'"{t}"' for t in tables])
        c_rules = " | ".join([f'"{c}"' for c in columns])

        grammar = f"""root ::= query
query ::= select (";")?
select ::= "SELECT " ( "DISTINCT " )? collist " FROM " tableref (joinclause)* (whereclause)? (groupclause)? (orderclause)? (limitclause)?
collist ::= colitem (", " colitem)*
colitem ::= (colname | "*") ( " AS " [a-zA-Z] [a-zA-Z0-9]* )? | funccall
tableref ::= tablename ( " AS " [a-zA-Z] [a-zA-Z0-9]* )?
joinclause ::= " JOIN " tableref " ON " expr
whereclause ::= " WHERE " expr
groupclause ::= " GROUP BY " collist
orderclause ::= " ORDER BY " collist (" ASC" | " DESC")?
limitclause ::= " LIMIT " [0-9]+
tablename ::= {t_rules}
colname ::= {c_rules}
expr ::= term ( " " binop " " term )*
term ::= colname | tablename "." colname | stringlit | number | funccall | "(" expr ")"
binop ::= "=" | "!=" | "<" | ">" | "<=" | ">=" | "AND" | "OR" | "LIKE" | "ILIKE" | "IN"
funccall ::= ("COUNT" | "SUM" | "AVG" | "MAX" | "MIN") "(" (colname | "*") ")"
stringlit ::= "'" [^']* "'"
number ::= [0-9]+ ("." [0-9]+)?"""
        
        return grammar.strip()
