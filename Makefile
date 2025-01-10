# Makefile for compiling the 'myDB' project

# Compiler settings
CXX = g++
CXXFLAGS = -std=c++20 -Wall

# Directories
SRCDIR = src/src
INCLUDEDIR = src/include
LIBDIR = src/include/antlr4
OUTPUTDIR = bin
DATADIR = data
GLOBALDIR = data/global
BASEDIR = data/base

# Include directories
INCLUDES = -I$(INCLUDEDIR) -I$(LIBDIR) \
           -I$(INCLUDEDIR)/antlr4/atn \
           -I$(INCLUDEDIR)/antlr4/dfa \
           -I$(INCLUDEDIR)/antlr4/internal \
           -I$(INCLUDEDIR)/antlr4/misc \
           -I$(INCLUDEDIR)/antlr4/support \
           -I$(INCLUDEDIR)/antlr4/tree

# Source files
SRCS = $(wildcard $(SRCDIR)/*/*.cpp) \
       $(wildcard $(LIBDIR)/*.cpp) \
       $(wildcard $(LIBDIR)/*/*.cpp)

# Output executable
TARGET = myDB

# Target to build the executable
all: $(TARGET)

$(TARGET): $(SRCS)
	@echo "Building the project..."
	@mkdir -p $(OUTPUTDIR) $(DATADIR) $(GLOBALDIR) $(BASEDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) src/main.cpp $(SRCS) -o $(OUTPUTDIR)/$(TARGET)
	@echo "Build complete: $(OUTPUTDIR)/$(TARGET)"

# Clean target to remove generated files
clean:
	@echo "Cleaning up generated files..."
	@rm -rf $(OUTPUTDIR)/* $(DATADIR)
# clean up the executable as well.
	@rm -f $(OUTPUTDIR)/$(TARGET)
	@echo "Cleanup complete."
