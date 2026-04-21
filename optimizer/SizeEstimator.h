#ifndef SIZE_ESTIMATOR_H
#define SIZE_ESTIMATOR_H

#include "InMemoryDatabase.h"
#include "Catalog.h"

/**
 * SizeEstimator - Estimates result sizes for joins
 * Formula: (nr * ns) / max(v_left, v_right)
 * - nr, ns: number of rows in each table
 * - v_left, v_right: distinct values in join columns
 */
class SizeEstimator {
private:
    const Catalog* catalog;

public:
    SizeEstimator(const Catalog* c) : catalog(c) {}

    /**
     * Estimate size of joining two tables
     * @param leftTable  Name of left table
     * @param rightTable Name of right table
     * @param leftCol    Join column in left table
     * @param rightCol   Join column in right table
     * @return Estimated number of rows after join
     */
    int64_t estimateJoinSize(const std::string& leftTable,
                             const std::string& rightTable,
                             const std::string& leftCol,
                             const std::string& rightCol) const {
        int64_t nr = catalog->getRowCount(leftTable);
        int64_t ns = catalog->getRowCount(rightTable);

        int64_t vLeft = catalog->getDistinct(leftTable, leftCol);
        int64_t vRight = catalog->getDistinct(rightTable, rightCol);

        // Use max distinct value for conservative estimate
        int64_t maxV = (vLeft > vRight) ? vLeft : vRight;
        if (maxV <= 0) maxV = 1;  // Avoid division by zero

        return (nr * ns) / maxV;
    }
};

#endif
