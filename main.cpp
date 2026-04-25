#include <iostream>
#include <fstream>
#include <sstream>
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
#include "optimizer/ExecutionEngine.h"

// =============================================================================
// TeeStreambuf: writes to two streams simultaneously (terminal + file)
// =============================================================================
class TeeStreambuf : public std::streambuf {
    std::streambuf* primary;   // e.g. cout's buffer (terminal)
    std::streambuf* secondary; // e.g. file buffer
public:
    TeeStreambuf(std::streambuf* p, std::streambuf* s) : primary(p), secondary(s) {}

    int overflow(int c) override {
        if (c == EOF) return !EOF;
        primary->sputc(c);
        secondary->sputc(c);
        return c;
    }

    std::streamsize xsputn(const char* s, std::streamsize n) override {
        primary->sputn(s, n);
        secondary->sputn(s, n);
        return n;
    }
};

// =============================================================================
// Helpers
// =============================================================================

void collectTableNames(std::shared_ptr<PlanNode> node, std::vector<std::string>& tables) {
    if (!node) return;
    if (node->type == SCAN) {
        tables.push_back(node->tableName);
    } else if (node->type == JOIN) {
        collectTableNames(node->left, tables);
        collectTableNames(node->right, tables);
    }
}

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

// =============================================================================
// runTestQuery: runs a SQL query through optimizer + execution engine
// All output goes to std::cout (which may be tee'd to a file by the caller)
// =============================================================================
void runTestQuery(const std::string& query, const Optimizer& opt, const Catalog& cat) {
    std::cout << "\n==================================================================\n";
    std::cout << "TEST QUERY: " << query << "\n";
    std::cout << "------------------------------------------------------------------\n";

    // 1. Parse SQL to RA Tree
    RANode* raTree = parseSQLToRA(query);
    if (!raTree) {
        std::cout << "Parse failed!\n";
        return;
    }

    // 2. Build JoinGraph from query (string-based extraction)
    JoinGraph graph;

    if (query.find("students")          != std::string::npos) graph.base_tables.push_back("students");
    if (query.find("grades")            != std::string::npos) graph.base_tables.push_back("grades");
    if (query.find("courses")           != std::string::npos) graph.base_tables.push_back("courses");
    if (query.find("enrollments")       != std::string::npos) graph.base_tables.push_back("enrollments");
    if (query.find("huge_users")        != std::string::npos) graph.base_tables.push_back("huge_users");
    if (query.find("huge_transactions") != std::string::npos) graph.base_tables.push_back("huge_transactions");

    if (query.find("students.id = grades.student_id")         != std::string::npos)
        graph.join_conditions.emplace_back("students", "id", "grades", "student_id");
    if (query.find("enrollments.course_id = courses.id")      != std::string::npos)
        graph.join_conditions.emplace_back("enrollments", "course_id", "courses", "id");
    if (query.find("students.id = enrollments.student_id")    != std::string::npos)
        graph.join_conditions.emplace_back("students", "id", "enrollments", "student_id");
    if (query.find("huge_users.id = huge_transactions.user_id") != std::string::npos)
        graph.join_conditions.emplace_back("huge_users", "id", "huge_transactions", "user_id");

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

        // --- Execute and print ALL rows ---
        std::cout << "\n--- EXECUTION ENGINE RESULTS ---\n";
        InMemoryDatabase execDb;
        executeAndPrintQuery(*result.plan, execDb, std::cout);
        std::cout << "-------------------------------\n";
    } else {
        std::cout << "Optimizer could not find a valid plan.\n";
    }

    delete raTree;
}

// =============================================================================
// main
// =============================================================================
int main(int argc, char* argv[]) {
    InMemoryDatabase db;
    Catalog catalog(&db);
    SizeEstimator sizeEst(&catalog);
    CostModel costModel;
    Optimizer optimizer(&catalog, &sizeEst, &costModel);

    std::string testFile = "test.txt";

    // Single-query mode: ./optimizer_engine "SELECT ..."
    if (argc >= 2) {
        std::cout << "--- Running single query ---\n";
        catalog.printStats();
        runTestQuery(argv[1], optimizer, catalog);
        return 0;
    }

    // Batch mode: read from test.txt
    std::ifstream infile(testFile);
    if (!infile.is_open()) {
        std::cerr << "Could not open " << testFile << ". Running built-in default.\n";
        catalog.printStats();
        runTestQuery(
            "SELECT * FROM students JOIN grades ON students.id = grades.student_id WHERE students.age = 20",
            optimizer, catalog
        );
        return 0;
    }

    std::cout << "--- DBMS Heuristic Optimizer: Reading queries from " << testFile << " ---\n";
    catalog.printStats();

    std::string line;
    int lineNum   = 0;
    int queryCount = 0;

    while (std::getline(infile, line)) {
        lineNum++;

        // Skip blank lines and comments
        if (line.empty() || line[0] == '#') continue;

        size_t sep = line.find('|');
        if (sep == std::string::npos) {
            std::cerr << "[Line " << lineNum << "] Skipping (no '|' separator): " << line << "\n";
            continue;
        }

        std::string label = line.substr(0, sep);
        std::string query = line.substr(sep + 1);

        auto trim = [](std::string& s) {
            size_t start = s.find_first_not_of(" \t\r");
            size_t end   = s.find_last_not_of(" \t\r");
            s = (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
        };
        trim(label);
        trim(query);
        if (query.empty()) continue;

        queryCount++;
        std::cout << "\n[" << queryCount << "] Label: " << label << "\n";

        // Open output file for this label
        std::ofstream outfile(label + ".out");

        if (outfile.is_open()) {
            // Tee: redirect cout so it writes to BOTH the terminal and the .out file
            std::streambuf* originalCout = std::cout.rdbuf();
            TeeStreambuf tee(originalCout, outfile.rdbuf());
            std::cout.rdbuf(&tee);

            runTestQuery(query, optimizer, catalog);

            // Restore cout to terminal only
            std::cout.rdbuf(originalCout);
            outfile.close();
        } else {
            // Can't open file — just print to terminal
            std::cerr << "Warning: could not open " << label << ".out for writing\n";
            runTestQuery(query, optimizer, catalog);
        }
    }

    infile.close();

    if (queryCount == 0) {
        std::cout << "No valid queries found in " << testFile << ".\n";
    } else {
        std::cout << "\n--- Done. Ran " << queryCount << " queries from " << testFile << " ---\n";
    }

    return 0;
}
