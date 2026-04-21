#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <string>
#include <vector>
#include <map>
#include <limits>
#include <iostream>
#include "InMemoryDatabase.h"
#include "Catalog.h"
#include "SizeEstimator.h"
#include "CostModel.h"
#include "JoinGraph.h"

/**
 * DPState - State in the DP table for a subset of tables
 */
struct DPState {
    int mask;
    double cost;
    int64_t size;
    std::string plan;
    std::string bestAlg;

    DPState() : mask(0), cost(0), size(0) {}
    DPState(int m, double c, int64_t s, const std::string& p, const std::string& a)
        : mask(m), cost(c), size(s), plan(p), bestAlg(a) {}
};

/**
 * Optimizer - Bitmask DP to find optimal join order
 */
class Optimizer {
private:
    const Catalog* catalog;
    SizeEstimator* sizeEst;
    CostModel* costModel;

    // Get table names in a mask
    std::vector<std::string> getTablesInMask(const std::vector<std::string>& tables, int mask) const {
        std::vector<std::string> result;
        for (int i = 0; i < (int)tables.size(); i++) {
            if (mask & (1 << i)) {
                result.push_back(tables[i]);
            }
        }
        return result;
    }

public:
    Optimizer(const Catalog* c, SizeEstimator* se, CostModel* cm)
        : catalog(c), sizeEst(se), costModel(cm) {}

    DPState optimize(const JoinGraph& graph) {
        const std::vector<std::string>& tables = graph.base_tables;
        const std::vector<JoinPredicate>& predicates = graph.join_conditions;

        int n = tables.size();
        if (n == 0) return DPState();

        std::map<int, DPState> dp;

        // Base case: single tables
        for (int i = 0; i < n; i++) {
            int mask = 1 << i;
            std::string tableName = tables[i];
            int64_t rows = catalog->getRowCount(tableName);

            dp[mask] = DPState(mask, 0.0, rows, tableName, "SCAN");
        }

        // Fill DP table for all subsets (process by increasing bit count)
        for (int bits = 2; bits <= n; bits++) {
            for (int mask = 1; mask < (1 << n); mask++) {
                int bitCount = 0;
                for (int tmp = mask; tmp > 0; tmp >>= 1) {
                    bitCount += (tmp & 1);
                }
                if (bitCount != bits) continue;

                double minCost = std::numeric_limits<double>::max();
                int64_t minSize = 0;
                std::string bestPlan = "";
                std::string bestAlg = "";
                bool found = false;

                // Enumerate all ways to split mask into two non-empty submasks
                for (int submask = mask & (mask - 1); submask > 0; submask = (submask - 1) & mask) {
                    int rightMask = mask ^ submask;
                    if (rightMask == 0) continue;

                    // Check if both submasks have valid plans (they must be joinable)
                    if (dp.find(submask) == dp.end() || dp[submask].plan == "") {
                        continue;
                    }
                    if (dp.find(rightMask) == dp.end() || dp[rightMask].plan == "") {
                        continue;
                    }

                    // Check if there's a predicate connecting these two groups
                    // Track which predicate tables are in which group
                    bool t1_in_sub = false, t2_in_sub = false;
                    bool t1_in_right = false, t2_in_right = false;
                    std::string pred_t1, pred_t2, pred_c1, pred_c2;
                    bool foundPred = false;

                    for (const JoinPredicate& p : predicates) {
                        // Check if predicate connects submask and rightMask
                        bool p_t1_in_sub = false, p_t2_in_sub = false;
                        bool p_t1_in_right = false, p_t2_in_right = false;

                        for (int i = 0; i < (int)tables.size(); i++) {
                            if (submask & (1 << i)) {
                                if (tables[i] == p.table1) p_t1_in_sub = true;
                                if (tables[i] == p.table2) p_t2_in_sub = true;
                            }
                            if (rightMask & (1 << i)) {
                                if (tables[i] == p.table1) p_t1_in_right = true;
                                if (tables[i] == p.table2) p_t2_in_right = true;
                            }
                        }

                        if ((p_t1_in_sub && p_t2_in_right) || (p_t2_in_sub && p_t1_in_right)) {
                            pred_t1 = p.table1;
                            pred_t2 = p.table2;
                            pred_c1 = p.col1;
                            pred_c2 = p.col2;
                            t1_in_sub = p_t1_in_sub;
                            t2_in_sub = p_t2_in_sub;
                            t1_in_right = p_t1_in_right;
                            t2_in_right = p_t2_in_right;
                            foundPred = true;
                            break;
                        }
                    }

                    if (!foundPred) {
                        continue;
                    }

                    // Assign tables based on which side of predicate is in which group
                    std::string leftTable, rightTable, leftCol, rightCol;
                    if (t1_in_sub && t2_in_right) {
                        leftTable = pred_t1;
                        leftCol = pred_c1;
                        rightTable = pred_t2;
                        rightCol = pred_c2;
                    } else if (t2_in_sub && t1_in_right) {
                        leftTable = pred_t2;
                        leftCol = pred_c2;
                        rightTable = pred_t1;
                        rightCol = pred_c1;
                    } else {
                        continue;
                    }

                    // Calculate join size
                    int64_t leftRows = dp[submask].size;
                    int64_t rightRows = dp[rightMask].size;
                    int64_t joinSize = sizeEst->estimateJoinSize(leftTable, rightTable, leftCol, rightCol);

                    // Find best algorithm
                    double joinCost;
                    std::string alg = costModel->findBestAlgorithm(leftRows, rightRows, joinCost);

                    // Total cost
                    double totalCost = dp[submask].cost + dp[rightMask].cost + joinCost;

                    if (totalCost < minCost) {
                        minCost = totalCost;
                        minSize = joinSize;
                        bestPlan = "(" + dp[submask].plan + " " + alg + " " + dp[rightMask].plan + ")";
                        bestAlg = alg;
                        found = true;
                    }
                }

                if (found) {
                    dp[mask] = DPState(mask, minCost, minSize, bestPlan, bestAlg);
                }
            }
        }

        int finalMask = (1 << n) - 1;
        return dp[finalMask];
    }
};

#endif
