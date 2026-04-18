#include <iostream>
#include <vector>
#include <string>
#include "SQLToRATreeConverter.h"

int main() {
    // Test queries
    std::vector<std::string> queries;
    queries.push_back("SELECT * FROM mytable;");
    queries.push_back("SELECT id, name FROM students WHERE id = 1;");
    queries.push_back("SELECT * FROM orders WHERE amount > 100;");
    queries.push_back("SELECT * FROM students JOIN grades ON students.id = grades.student_id;");
    queries.push_back("SELECT * FROM students INNER JOIN grades ON students.id = grades.student_id;");
    queries.push_back("SELECT * FROM students LEFT JOIN grades ON students.id = grades.student_id;");
    queries.push_back("SELECT students.name, courses.title FROM students JOIN enrollments ON students.id = enrollments.student_id JOIN courses ON enrollments.course_id = courses.id;");
    queries.push_back("SELECT * FROM students JOIN grades ON students.id = grades.student_id WHERE grades.score > 90;");
    queries.push_back("SELECT a.x, b.y FROM table_a AS a JOIN table_b AS b ON a.id = b.ref_id;");

    for (size_t i = 0; i < queries.size(); ++i) {
        std::cout << "========================================" << std::endl;
        std::cout << "Query: " << queries[i] << std::endl;
        std::cout << "----------------------------------------" << std::endl;

        RANode* raTree = parseSQLToRA(queries[i]);

        if (raTree) {
            std::cout << "RA Tree:" << std::endl;
            std::cout << raTree->toString() << std::endl;

            // Clean up
            delete raTree;

            std::cout << "----------------------------------------" << std::endl;
            std::cout << "Parse successful!" << std::endl;
        } else {
            std::cout << "Failed to build RA tree" << std::endl;
        }
        std::cout << "========================================" << std::endl << std::endl;
    }

    return 0;
}
