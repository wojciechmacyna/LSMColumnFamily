# Variables
# CXX = clang++


# CXXFLAGS = -std=c++20  -I/usr/local/include/rocksdb -Iinclude -Ibloom -I/usr/include/spdlog
# LDFLAGS = -L/usr/local/lib  -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl -fvisibility=hidden -fvisibility-inlines-hidden -lrocksdb -lfmt -lboost_system -lboost_thread -no-pie

# ----------Local
#rm -rf obj
#rm -f HierarchicalDB

#make -j"$(nproc)" \
 #   ROCKSDB_DIR=/usr \
 #   ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu

# ------------ Remote
#rm -rf obj
#rm -f HierarchicalDB

##make -j"$(nproc)" \
 #   ROCKSDB_DIR=/home/wmacyna/rocksDB/rocksdb2 \
 #   ROCKSDB_LIB_DIR=/home/wmacyna/rocksDB/rocksdb2



CXX = clang++

ROCKSDB_DIR ?= /usr
ROCKSDB_LIB_DIR ?= $(ROCKSDB_DIR)/lib

CXXFLAGS = -std=c++20 \
           -I$(ROCKSDB_DIR)/include \
           -Iinclude -Ibloom -I/usr/include/spdlog

LDFLAGS = -L$(ROCKSDB_LIB_DIR) \
          -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl \
          -fvisibility=hidden -fvisibility-inlines-hidden \
          -lrocksdb -lfmt -lboost_system -lboost_thread -no-pie

TARGET = HierarchicalDB
SRC = \
    src/db_manager.cpp \
    src/bloom_manager.cpp \
    src/main.cpp \
    src/exp1.cpp \
    src/exp5.cpp \
    src/exp6.cpp \
    src/exp7.cpp \
    src/exp8.cpp \
    src/expJ1.cpp \
    src/expJ2.cpp \
    src/expJ_real_data.cpp\
    src/exp_utils.cpp \
    bloom/bloom_tree.cpp \
    bloom/bloom_filter.cpp \
    bloom/node.cpp \
    bloom/MurmurHash3.cpp \
    src/sst_hash_index.cpp \
    src/real_data.cpp

# Convert source files to object files
OBJ_DIR = obj
OBJ = $(SRC:%.cpp=$(OBJ_DIR)/%.o)

# Create object directories
$(shell mkdir -p $(sort $(dir $(OBJ))))

# Build target
$(TARGET): $(OBJ)
	$(CXX) -fno-rtti -o $@ $^ $(LDFLAGS)

# Pattern rule for object files
$(OBJ_DIR)/%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target
clean:
	rm -f $(TARGET)
	rm -rf $(OBJ_DIR)
	rm -rf db

.PHONY: clean

