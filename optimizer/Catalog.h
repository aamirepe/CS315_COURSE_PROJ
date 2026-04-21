#ifndef CATALOG_H
#define CATALOG_H

#include "InMemoryDatabase.h"
#include <string>
#include <map>

/**
 * Catalog - Stores table statistics for the optimizer
 * Uses InMemoryDatabase to generate stats automatically
 */
class Catalog {
private:
    const InMemoryDatabase* db;

public:
    Catalog(const InMemoryDatabase* database) : db(database) {}

    // Get row count for a table
    int64_t getRowCount(const std::string& tableName) const {
        return db->getRowCount(tableName);
    }

    // Get distinct value count for a column in a table
    int64_t getDistinct(const std::string& tableName, const std::string& colName) const {
        return db->getDistinctCount(tableName, colName);
    }

    // Print all statistics
    void printStats() const {
        std::cout << "\n=== Catalog Statistics ===" << std::endl;
        std::cout << "students: " << getRowCount("students") << " rows" << std::endl;
        std::cout << "  distinct id: " << getDistinct("students", "id") << std::endl;
        std::cout << "grades: " << getRowCount("grades") << " rows" << std::endl;
        std::cout << "  distinct student_id: " << getDistinct("grades", "student_id") << std::endl;
        std::cout << "  distinct course: " << getDistinct("grades", "course") << std::endl;
        std::cout << "enrollments: " << getRowCount("enrollments") << " rows" << std::endl;
        std::cout << "  distinct student_id: " << getDistinct("enrollments", "student_id") << std::endl;
        std::cout << "  distinct course_id: " << getDistinct("enrollments", "course_id") << std::endl;
        std::cout << "courses: " << getRowCount("courses") << " rows" << std::endl;
        std::cout << "  distinct id: " << getDistinct("courses", "id") << std::endl;
        std::cout << "==========================\n" << std::endl;
    }
};

#endif
