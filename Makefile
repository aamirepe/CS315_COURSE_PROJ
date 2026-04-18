CFLAGS = -std=c++1z -lstdc++ -Wall -Werror -I. -Isql-parser/src/ -Lsql-parser/

all:
	$(CXX) $(CFLAGS) test_ra_tree.cpp SQLToRATreeConverter.cpp -o test_ra_tree -lsqlparser
