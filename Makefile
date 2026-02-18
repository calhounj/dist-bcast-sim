CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -O0 -g
INCLUDES = -Iinclude

TARGET = build/sim
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	mkdir -p build
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TARGET) $(SRC)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf build

