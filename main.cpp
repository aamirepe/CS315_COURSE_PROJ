#include <iostream>
#include <string>
#include <vector>
#include "SQLParser.h"

// =============================================================================
// Relational Algebra Tree (RA Tree) Node Structures
// =============================================================================

struct RANode {
    virtual ~RANode() = default;
    virtual std::string toString(int indent = 0) const = 0;
};

// Represents a base table in the query
struct TableNode : RANode {
    std::string tableName;

    TableNode(std::string name) : tableName(name) {}

    std::string toString(int indent = 0) const override {
        std::string spaces(indent, ' ');
        return spaces + "TableNode: " + tableName;
    }
};

// Represents a projection (SELECT columns)
struct ProjectNode : RANode {
    std::vector<RANode*> children;
    std::vector<std::string> columns;

    ProjectNode(std::vector<std::string> cols) : columns(cols) {}

    ~ProjectNode() override {
        for (auto child : children) {
            delete child;
        }
    }

    std::string toString(int indent = 0) const override {
        std::string spaces(indent, ' ');
        std::string result = spaces + "ProjectNode: [";
        for (size_t i = 0; i < columns.size(); ++i) {
            result += columns[i];
            if (i < columns.size() - 1) result += ", ";
        }
        result += "]\n";
        for (auto child : children) {
            result += child->toString(indent + 2) + "\n";
        }
        return result;
    }
};

// Represents a selection (WHERE clause - filter)
struct SelectNode : RANode {
    RANode* child;
    std::string condition;

    SelectNode(RANode* c, std::string cond) : child(c), condition(cond) {}

    ~SelectNode() override {
        delete child;
    }

    std::string toString(int indent = 0) const override {
        std::string spaces(indent, ' ');
        std::string result = spaces + "SelectNode: WHERE " + condition + "\n";
        result += child->toString(indent + 2);
        return result;
    }
};

// Represents a JOIN operation
struct JoinNode : RANode {
    RANode* left;
    RANode* right;
    std::string joinCondition;

    JoinNode(RANode* l, RANode* r, std::string cond) : left(l), right(r), joinCondition(cond) {}

    ~JoinNode() override {
        delete left;
        delete right;
    }

    std::string toString(int indent = 0) const override {
        std::string spaces(indent, ' ');
        std::string result = spaces + "JoinNode: JOIN ON " + joinCondition + "\n";
        result += left->toString(indent + 2) + "\n";
        result += right->toString(indent + 2);
        return result;
    }
};

// =============================================================================
// SQL to RA Tree Converter
// =============================================================================

// Helper to extract column names from Expr vector
std::vector<std::string> extractColumns(std::vector<hsql::Expr*>* exprList) {
    std::vector<std::string> cols;
    if (!exprList) return cols;

    for (auto expr : *exprList) {
        if (!expr) continue;

        switch (expr->type) {
            case hsql::kExprStar:
                cols.push_back("*");
                break;
            case hsql::kExprColumnRef: {
                std::string colName = expr->name ? expr->name : "";
                if (expr->table && expr->table[0] != '\0') {
                    colName = std::string(expr->table) + "." + colName;
                }
                cols.push_back(colName);
                break;
            }
            case hsql::kExprLiteralInt: {
                std::string val = std::to_string(expr->ival);
                cols.push_back(val);
                break;
            }
            case hsql::kExprLiteralFloat: {
                std::string val = std::to_string(expr->fval);
                cols.push_back(val);
                break;
            }
            case hsql::kExprLiteralString: {
                std::string val = expr->name ? std::string(expr->name) : "";
                cols.push_back("'" + val + "'");
                break;
            }
            default:
                cols.push_back("<expr>");
                break;
        }
    }
    return cols;
}

// Helper to extract WHERE clause condition as string
std::string extractCondition(hsql::Expr* expr) {
    if (!expr) return "";

    switch (expr->type) {
        case hsql::kExprColumnRef: {
            std::string result = expr->table ? expr->table : "";
            if (!result.empty()) result += ".";
            result += expr->name ? expr->name : "";
            return result;
        }
        case hsql::kExprLiteralInt:
            return std::to_string(expr->ival);
        case hsql::kExprLiteralFloat:
            return std::to_string(expr->fval);
        case hsql::kExprLiteralString: {
            std::string val = expr->name ? expr->name : "";
            return "'" + val + "'";
        }
        case hsql::kExprOperator: {
            std::string left = extractCondition(expr->expr);
            std::string right = extractCondition(expr->expr2);
            std::string op;
            switch (expr->opType) {
                case hsql::kOpEquals: op = "="; break;
                case hsql::kOpNotEquals: op = "!="; break;
                case hsql::kOpLess: op = "<"; break;
                case hsql::kOpLessEq: op = "<="; break;
                case hsql::kOpGreater: op = ">"; break;
                case hsql::kOpGreaterEq: op = ">="; break;
                case hsql::kOpAnd: op = "AND"; break;
                case hsql::kOpOr: op = "OR"; break;
                default: op = "?"; break;
            }
            return "(" + left + " " + op + " " + right + ")";
        }
        default:
            return "<condition>";
    }
}

// Main conversion function - parses SQL and returns RA tree
RANode* parseSQLToRA(const std::string& query) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    if (!result.isValid() || result.size() == 0) {
        std::cerr << "Parse failed: " << result.errorMsg() << std::endl;
        return nullptr;
    }

    // Handle only SELECT statements for now
    auto stmt = result.getStatement(0);
    if (!stmt->isType(hsql::kStmtSelect)) {
        std::cerr << "Only SELECT statements supported in this demo" << std::endl;
        return nullptr;
    }

    auto selectStmt = static_cast<const hsql::SelectStatement*>(stmt);

    // Build RA tree bottom-up

    // 1. Create TableNode for FROM clause
    TableNode* tableNode = nullptr;
    if (selectStmt->fromTable && selectStmt->fromTable->name) {
        tableNode = new TableNode(selectStmt->fromTable->name);
    }

    // 2. Create ProjectNode for SELECT columns
    std::vector<std::string> selectCols = extractColumns(selectStmt->selectList);
    ProjectNode* projectNode = new ProjectNode(selectCols);
    if (tableNode) {
        projectNode->children.push_back(tableNode);
    }

    // 3. Create SelectNode for WHERE clause if present
    if (selectStmt->whereClause) {
        std::string whereCond = extractCondition(selectStmt->whereClause);
        SelectNode* selectNode = new SelectNode(projectNode, whereCond);
        return selectNode;
    }

    return projectNode;
}

// =============================================================================
// Main
// =============================================================================

int main() {
    // Test queries
    std::vector<std::string> queries = {
        "SELECT * FROM mytable;",
        "SELECT id, name FROM students WHERE id = 1;",
        "SELECT * FROM orders WHERE amount > 100;"
    };

    for (const auto& query : queries) {
        std::cout << "========================================" << std::endl;
        std::cout << "Query: " << query << std::endl;
        std::cout << "----------------------------------------" << std::endl;

        RANode* raTree = parseSQLToRA(query);

        if (raTree) {
            std::cout << "RA Tree:" << std::endl;
            std::cout << raTree->toString();
            std::cout << "----------------------------------------" << std::endl;
            std::cout << "Parse successful!" << std::endl;
        } else {
            std::cout << "Failed to build RA tree" << std::endl;
        }
        std::cout << "========================================" << std::endl << std::endl;
    }

    return 0;
}
