#ifndef COST_MODEL_H
#define COST_MODEL_H

#include <string>
#include <algorithm>

/**
 * CostModel - Calculates simple cost estimates for join algorithms
 * Uses in-memory heuristics (not disk I/O based):
 * - Hash Join cost: 3 * (rows_left + rows_right)
 * - BNLJ cost: rows_left + rows_right
 * - Merge cost: rows_left + rows_right
 *
 * For course project: simple heuristics work fine.
 */
class CostModel {
public:
    /**
     * Estimate cost of hash join
     * Heuristic: 3 * (rows_left + rows_right)
     * - Phase 1: Read both tables
     * - Phase 2: Build hash table
     * - Phase 3: Probe and output
     */
    double hashJoinCost(int64_t rowsLeft, int64_t rowsRight) const {
        return 3.0 * (rowsLeft + rowsRight);
    }

    /**
     * Estimate cost of BNLJ (Block Nested Loop Join)
     * Heuristic: sum of both table sizes
     */
    double bnljCost(int64_t rowsLeft, int64_t rowsRight) const {
        return rowsLeft + rowsRight;
    }

    /**
     * Estimate cost of Merge Join
     * Heuristic: sum of both table sizes
     */
    double mergeJoinCost(int64_t rowsLeft, int64_t rowsRight) const {
        return rowsLeft + rowsRight;
    }

    /**
     * Find best algorithm and return its cost
     */
    std::string findBestAlgorithm(int64_t rowsLeft, int64_t rowsRight,
                                  double& minCost) const {
        double cHash = hashJoinCost(rowsLeft, rowsRight);
        double cBnlj = bnljCost(rowsLeft, rowsRight);
        double cMerge = mergeJoinCost(rowsLeft, rowsRight);

        minCost = cHash;
        std::string bestAlg = "Hash";

        if (cMerge < minCost) {
            minCost = cMerge;
            bestAlg = "Merge";
        }
        if (cBnlj < minCost) {
            minCost = cBnlj;
            bestAlg = "BNLJ";
        }

        return bestAlg;
    }
};

#endif
