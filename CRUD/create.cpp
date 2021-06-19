#include <vector>
#include <set>
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../main.h"
#include "../database.h"

void CRUD::create(nlohmann::json& documents, nlohmann::json& response)
{
	std::vector<size_t> enteredIds = {};
	for (const auto& item : documents.items()) {
		const std::string colName = item.key();
		const nlohmann::json docArray = item.value();

		if (!docArray.is_array())
			continue;

		// Create Collection when needed
		if (!collections.count(colName)) {
			std::filesystem::create_directories(DATA_PATH + "/col_" + colName);
			collections[colName] = new collection_t(colName);
			saveDatabase(DATA_PATH);
		}

		// Enter all Documents and insert new Ids
		std::vector<size_t> curIds = collections[colName]->insertDocuments(docArray.get<std::vector<nlohmann::json>>());
		enteredIds.insert(enteredIds.end(), curIds.begin(), curIds.end());	
	}

	response = {
		{"status", "ok"},
		{"newIds", enteredIds}
	};
}