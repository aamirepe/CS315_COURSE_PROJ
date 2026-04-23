#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include "SQLParser.h"
#include "SQLToRATreeConverter.h"
#include "optimizer/InMemoryDatabase.h"
#include "optimizer/Catalog.h"
#include "optimizer/SizeEstimator.h"
#include "optimizer/CostModel.h"
#include "optimizer/Optimizer.h"
#include "optimizer/JoinGraph.h"
#include "optimizer/PlanNode.h"

// Helper function to collect table names from a plan subtree
void collectTableNames(std::shared_ptr<PlanNode> node, std::vector<std::string>& tables) {
    if (!node) return;
    if (node->type == SCAN) {
        tables.push_back(node->tableName);
    } else if (node->type == JOIN) {
        collectTableNames(node->left, tables);
        collectTableNames(node->right, tables);
    }
}

// Helper function to print the join sequence
void printJoinSequence(std::shared_ptr<PlanNode> node, int& joinCounter) {
    if (!node) return;

    if (node->type == JOIN) {
        printJoinSequence(node->left, joinCounter);
        printJoinSequence(node->right, joinCounter);

        std::cout << "  Join " << ++joinCounter << ": ";

        std::vector<std::string> leftTables, rightTables;
        collectTableNames(node->left, leftTables);
        collectTableNames(node->right, rightTables);

        std::cout << "[";
        for (size_t i = 0; i < leftTables.size(); i++) {
            if (i > 0) std::cout << ",";
            std::cout << leftTables[i];
        }
        std::cout << "] X [";
        for (size_t i = 0; i < rightTables.size(); i++) {
            if (i > 0) std::cout << ",";
            std::cout << rightTables[i];
        }
        std::cout << "] ON " << node->condition;

        std::string algStr = (node->algorithm == HASH) ? "HashJoin" :
                             (node->algorithm == BNLJ) ? "BNLJ" : "MergeJoin";
        std::cout << " (" << algStr << ")" << std::endl;
    }
}

void runTestQuery(const std::string& query, const Optimizer& opt, const Catalog& cat) {
    std::cout << "\n==================================================================\n";
    std::cout << "TEST QUERY: " << query << "\n";
    std::cout << "------------------------------------------------------------------\n";

    // 1. Parse SQL to RA Tree (to get basic structure)
    RANode* raTree = parseSQLToRA(query);
    if (!raTree) {
        std::cout << "Parse failed!\n";
        return;
    }

    // 2. Build JoinGraph from query
    // In a full system, the SQLToRATreeConverter would do this.
    // For testing, we manually simulate the extraction of tables and conditions.
    JoinGraph graph;

    // Simple logic to extract tables for our test cases
    if (query.find("students") != std::string::npos) graph.base_tables.push_back("students");
    if (query.find("grades") != std::string::npos) graph.base_tables.push_back("grades");
    if (query.find("courses") != std::string::npos) graph.base_tables.push_back("courses");
    if (query.find("enrollments") != std::string::npos) graph.base_tables.push_back("enrollments");

    // Mocking join conditions based on common test patterns
    if (query.find("students.id = grades.student_id") != std::string::npos) {
        graph.join_conditions.emplace_back("students", "id", "grades", "student_id");
    }
    if (query.find("enrollments.course_id = courses.id") != std::string::npos) {
        graph.join_conditions.emplace_back("enrollments", "course_id", "courses", "id");
    }
    if (query.find("students.id = enrollments.student_id") != std::string::npos) {
        graph.join_conditions.emplace_back("students", "id", "enrollments", "student_id");
    }

    // Mocking Selection conditions (WHERE clause)
    if (query.find("WHERE") != std::string::npos) {
        size_t pos = query.find("WHERE");
        graph.selection_conditions.push_back(query.substr(pos + 6));
    }

    std::cout << "Optimizer is processing heuristics and DP...\n";
    DPState result = opt.optimize(graph);

    if (result.plan) {
        std::cout << "\nOPTIMAL PHYSICAL PLAN (Tree):\n";
        result.plan->print();

        std::cout << "\nJOIN SEQUENCE (Order of Operations):\n";
        int joinCounter = 0;
        printJoinSequence(result.plan, joinCounter);

        std::cout << "\nEstimated Cost: " << result.cost << "\n";
        std::cout << "Resulting Tuple Size: " << result.size << "\n";
        std::cout << "Best Algorithm Selected: " << result.bestAlg << "\n";
    } else {
        std::cout << "Optimizer could not find a valid plan.\n";
    }

    // Cleanup RA tree
    // Note: In a real system, use a proper smart pointer or recursive delete
}

int main() {
    // Setup Database and Catalog
    InMemoryDatabase db;
    Catalog catalog(&db);
    SizeEstimator sizeEst(&catalog);
    CostModel costModel;
    Optimizer optimizer(&catalog, &sizeEst, &costModel);

    std::cout << "--- DBMS Heuristic Optimizer Test Suite ---\n";
    catalog.printStats();

    // Test Case 1: Simple Join with Filter (Tests Selection Push-Down)
    runTestQuery(
        "SELECT * FROM students JOIN grades ON students.id = grades.student_id WHERE students.age = 20",
        optimizer, catalog
    );

    // Test Case 2: 3-Table Join (Tests Join Ordering and Left-Deep constraint)
    runTestQuery(
        "SELECT * FROM students JOIN enrollments ON students.id = enrollments.student_id JOIN courses ON enrollments.course_id = courses.id",
        optimizer, catalog
    );

    // Test Case 3: Cross Product (Tests Penalty Heuristic)
    // Note: Using comma syntax as the sql-parser doesn't support CROSS JOIN keyword
    runTestQuery(
        "SELECT * FROM students, courses",
        optimizer, catalog
    );

    // Test Case 4: 4-Table Join with Multiple Conditions (Complex Query)
    runTestQuery(
        "SELECT * FROM students "
        "JOIN grades ON students.id = grades.student_id "
        "JOIN enrollments ON students.id = enrollments.student_id "
        "JOIN courses ON enrollments.course_id = courses.id "
        "WHERE students.age > 18",
        optimizer, catalog
    );

    // Test Case 5: 4-Table Join with Different Join Order
    runTestQuery(
        "SELECT * FROM courses "
        "JOIN enrollments ON courses.id = enrollments.course_id "
        "JOIN students ON enrollments.student_id = students.id "
        "JOIN grades ON students.id = grades.student_id",
        optimizer, catalog
    );

    return 0;
}
