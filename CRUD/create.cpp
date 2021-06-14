#include <vector>
#include <set>
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../main.h"
#include "../database.h"

nlohmann::json CRUD::create(nlohmann::json& documents)
{
	std::vector<size_t> enteredIds = {};
	for (const auto& item : documents.items()) {
		const std::string colName = item.key();
		const nlohmann::json docArray = item.value();

		if (!docArray.is_array())
			continue;

		// Creat Collection when needed
		if (!collections.count(colName))
			collections[colName] = new collection_t(colName);

		// Enter all Documents and insert new Ids
		std::vector<size_t> curIds = collections[colName]->insertDocuments(docArray.get<std::vector<nlohmann::json>>());
		enteredIds.insert(enteredIds.end(), curIds.begin(), curIds.end());	
	}

	return nlohmann::json({
		{"status", "ok"},
		{"newIds", enteredIds}
		});
}