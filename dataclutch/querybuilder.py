import operator as op
from typing import List, Any, Tuple


# Mongodb用マッピング

operator_map = {
    'not': '$not',
    'and': '$and',
    'or': '$or',
    '==': '$eq',
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
    'match': '$regex'
}

# ネストされたリスト形式の演算式を解析してMongoDBのクエリに変換する再帰関数
def parse_expression(expr):
    assert not isinstance(expr, list), "expr must use a Tuple, not a List"
    if not isinstance(expr, tuple):  # 基本的な変数または値の場合
        return expr

    op = expr[0]
    if op in operator_map:
        mongo_op = operator_map[op]
        if op == 'not':  # 単項演算子
            return {mongo_op: parse_expression(expr[1])}
        elif op in ['and', 'or']:  # 二項演算子
            conditions = [parse_expression(sub_expr) for sub_expr in expr[1:]]
            return {mongo_op: conditions}
        elif op in ['==', '>', '<', '>=', '<=', 'match']:  # 比較演算子
            field = expr[1]
            value = parse_expression(expr[2])
            return {field: {mongo_op: value}}
    else:
        raise ValueError(f'Unknown operator: {op}')

def find_expression(expr):
    if expr == None or len(expr) == 0:
        return {}
    if expr[0] == '==' and not isinstance(expr[1], tuple) and not isinstance(expr[2], tuple):
        return {expr[1]: expr[2]}
    else:
        return parse_expression(expr)

class QueryBuilder(object):
    def __init__(self, expr) -> None:
        self.expr = find_expression(expr)    
    def getquery(self) -> tuple:
        return self.expr

if __name__ == '__main__':
    # 使用例
    #expression = ["==", "name", "john"]
    #print(parse_expression(expression))

    expression = None
    print(find_expression(expression))
    expression = ()
    print(find_expression(expression))

    expression = ("==", "name", "john")
    print(find_expression(expression))
    expression = ("and", ("==", "name", "john"), ("==", "family", "doe"))
    print(find_expression(expression))
    expression = (">=", "age", "20")
    print(find_expression(expression))
    #expression = ["or", [">", "x", "y"], ["and", ["<", "z", 10], ["==", "a", "b"]]]
    #expression = ["or", [">", "x", "y"], ["and", ["<", "z", 10], ["==", "a", "MY STRING"]]]
    #print(parse_expression(expression))
