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
     * Heuristic: 3 * (rowsLeft + rowsRight)
     */
    double hashJoinCost(int64_t rowsLeft, int64_t rowsRight) const {
        return 3.0 * (rowsLeft + rowsRight);
    }

    /**
     * Estimate cost of BNLJ (Block Nested Loop Join)
     * Heuristic: rowsLeft * rowsRight
     */
    double bnljCost(int64_t rowsLeft, int64_t rowsRight) const {
        return (double)rowsLeft * rowsRight;
    }

    /**
     * Estimate cost of Merge Join
     * Heuristic: sort cost + scan cost
     */
    double mergeJoinCost(int64_t rowsLeft, int64_t rowsRight, bool leftSorted, bool rightSorted) const {
        double cost = 0;
        if (rowsLeft > 0)
            cost += leftSorted ? (double)rowsLeft : (double)rowsLeft * std::log2(rowsLeft);
        if (rowsRight > 0)
            cost += rightSorted ? (double)rowsRight : (double)rowsRight * std::log2(rowsRight);
        cost += (double)(rowsLeft + rowsRight); // final merge scan pass
        return cost;
    }

    /**
     * Find best algorithm and return its cost
     */
    std::string findBestAlgorithm(int64_t rowsLeft, int64_t rowsRight,
                                  double& minCost, bool leftSorted, bool rightSorted) const {
        double cHash = hashJoinCost(rowsLeft, rowsRight);
        double cBnlj = bnljCost(rowsLeft, rowsRight);
        double cMerge = mergeJoinCost(rowsLeft, rowsRight, leftSorted, rightSorted);

        minCost = cMerge;
        std::string bestAlg = "Merge";

        if (cHash < minCost) {
            minCost = cHash;
            bestAlg = "Hash";
        }
        if (cBnlj < minCost) {
            minCost = cBnlj;
            bestAlg = "BNLJ";
        }

        return bestAlg;
    }
};

#endif
