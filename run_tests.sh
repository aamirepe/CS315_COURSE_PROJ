#!/bin/bash

# Usage:
#   ./run_tests.sh               → runs all queries from test.txt
#   ./run_tests.sh "SELECT ..."  → runs a single inline query

# Compile the project
echo "--- Compiling DBMS Engine ---"
clang++ -std=c++17 main.cpp SQLToRATreeConverter.cpp -I. -Ioptimizer -Isql-parser/src -Lsql-parser -lsqlparser -o optimizer_engine

# Check if compilation succeeded
if [ $? -ne 0 ]; then
    echo "Error: Compilation failed."
    exit 1
fi

# Set library path for macOS to find sql-parser
export DYLD_LIBRARY_PATH=./sql-parser:$DYLD_LIBRARY_PATH

if [ -n "$1" ]; then
    # Single query mode: pass the query as an argument
    echo "--- Running single query ---"
    ./optimizer_engine "$1"
else
    # Batch mode: reads from test.txt, cleans old .out files first
    rm -f *.out
    echo "--- Running all queries from test.txt ---"
    ./optimizer_engine
    echo "--- Done. ---"
fi
