#include "main.h"
#include "database.h"

int main() {
	loadDatabase(DATA_PATH);

	saveDatabase(DATA_PATH);
	return 0;  
}